package nl.grons.reactivethrift

import java.nio.charset.StandardCharsets

import nl.grons.reactivethrift.Protocol.TMessage
import nl.grons.reactivethrift.decoders.DecodeResult._
import nl.grons.reactivethrift.decoders._
import nl.grons.reactivethrift.encoder.EncodeResult.{ContinueEncode, Encoded}
import nl.grons.reactivethrift.encoder._
import nl.grons.reactivethrift.example.StructWriter
import uk.co.real_logic.agrona.{DirectBuffer, MutableDirectBuffer}

import scala.annotation.switch

object CompactProtocol extends Protocol {

  private val CompactProtocolId : Byte = 0x82.toByte
  private val CompactProtocolVersion: Byte = 1

  override def structDecoder[A](structBuilder: Factory[StructBuilder]): Decoder[A] = {
    new StructDecoder(structBuilder).trampolined
  }

  private val EmptyByteArray: Array[Byte] = Array.empty

  private val BooleanFieldTrueDecoder: Decoder[Boolean] = Decoder.unit(true)
  private val BooleanFieldFalseDecoder: Decoder[Boolean] = Decoder.unit(false)

  //
  // Protocol:
  // * VarInt32 with length of array
  // * the bytes
  //
  private val BinaryDecoder: Decoder[Array[Byte]] =
    VarInt32Decoder.decodeAndThen { case (length, buffer, readOffset) =>
      // TODO: add maximum length check
      if (length == 0) {
        Decoded(EmptyByteArray, buffer, readOffset)
      } else if (length > 0) {
        BytesDecoder(length).decode(buffer, readOffset)
      } else {
        DecodeFailure(s"Negative length: $length")
      }
    }

  private val BinaryEncoder: Encoder[Array[Byte]] = new Encoder[Array[Byte]] {
    override def encode(value: Array[Byte], buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      //noinspection VariablePatternShadow
      VarInt32Encoder
        .encode(value.length, buffer, writeOffset)
        .andThen { case (buffer, writeOffset) =>
          if (value.length == 0) {
            Encoded(buffer, writeOffset)
          } else {
            BytesEncoder
              .encode(value, buffer, writeOffset)
          }
        }
    }
  }

  private val StringDecoder: Decoder[String] =
    BinaryDecoder.map(binary => new String(binary, StandardCharsets.UTF_8))

  private val StringEncoder: Encoder[String] =
    BinaryEncoder.map(_.getBytes(StandardCharsets.UTF_8))

  /**
    * TMessage decoder.
    *
    * Protocol:
    * * byte 1: protocol id, fixed to `0x82`
    * * byte 2: version, fixed to `1` (highest 3 bits) and message type (lowest 5 bits)
    * * var int32: sequence id
    * * var int32: length of message name
    * * bytes: message name
    */
  private val TMessageDecoder: Decoder[TMessage] =
    Decoder
      .product4(Int8Decoder, Int8Decoder, VarInt32Decoder, StringDecoder)
      .decodeAndThen { case ((protocolId, versionInfo, sequenceId, name), buffer, readOffset) =>
        val version = versionInfo & 0x1F
        val messageType = ((versionInfo  & 0xE0) >> 5).toByte
        if (protocolId != CompactProtocolId) {
          DecodeFailure(s"Received TMessage with unsupported protocol $protocolId (only $CompactProtocolId is supported)")
        } else if (version != CompactProtocolVersion) {
          DecodeFailure(s"Received TMessage with unsupported version $version (only $CompactProtocolVersion is supported)")
        } else {
          Decoded(TMessage(name, messageType, sequenceId), buffer, readOffset)
        }
      }

  private val TMessageEncoder: Encoder[TMessage] =
    Encoder
      .product4(Int8Encoder, Int8Encoder, VarInt32Encoder, StringEncoder)
      .map { message =>
        val typeAndVersion = ((message.messageType << 5).toByte | CompactProtocolVersion).toByte
        (CompactProtocolId, typeAndVersion, message.sequenceId, message.name)
      }

  // Field header decoder

  private case class FieldHeader(fieldId: Short, fieldTypeId: Int) {
    def isStopField = fieldTypeId == 0
  }

  private class FieldHeaderDecoder(previousFieldId: Short) extends Decoder[FieldHeader] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[FieldHeader] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 1) {
        val byte0 = buffer.getByte(readOffset) & 0xff
        // First byte looks as follows:
        //  76543210
        // +--------+
        // |ddddtttt|
        // +--------+
        // d = field id delta (unsigned 4 bits), >0, for the short form, 0 for stop field and extended form
        // t = field type id (4 bits), 0 for stop field
        //
        if (byte0 == 0) {
          // Stop field
          Decoded(FieldHeader(0, 0), buffer, readOffset + 1)

        } else {
          val fieldIdDelta = (byte0 >> 4).toShort
          val fieldTypeId = byte0 & 0x0f

          if (isInvalidTypeId(fieldTypeId)) {
            DecodeFailure("Not supported type id: " + fieldTypeId)

          } else if (fieldIdDelta != 0) {
            // Short form (1 byte).
            val fieldId = (previousFieldId + fieldIdDelta).toShort
            Decoded(FieldHeader(fieldId, fieldTypeId), buffer, readOffset + 1)

          } else {
            // Extended form (2 to 5 bytes).
            //  76543210 76543210 76543210
            // +--------+--------+...+--------+
            // |0000tttt| field id            |
            // +--------+--------+...+--------+
            // t = field type id (4 bits), always >0
            // i = field id (signed 16 bits, encoded as zigzag int)
            //
            //noinspection VariablePatternShadow
            ZigZagInt16Decoder
              .decode(buffer, readOffset + 1)  // Note the `+1`.
              .andThen { case (fieldId, buffer, readOffset) =>
                Decoded(FieldHeader(fieldId, fieldTypeId), buffer, readOffset)
              }
          }
        }
      } else {
        // no data available at all
        DecodeInsufficientData(this)
      }
    }
  }

  private val FieldHeaderExtendedFormEncoder = Encoder.product(Int8Encoder, VarInt32Encoder)

  private def encodeStopFieldHeader(buffer: MutableDirectBuffer, writeOffset: Int) = Int8Encoder.encode(0, buffer, writeOffset)

  private class FieldHeaderEncoder(previousFieldId: Short) extends Encoder[FieldHeader] {
    override def encode(value: FieldHeader, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      if (value.isStopField) {
        // stop field
        encodeStopFieldHeader(buffer, writeOffset)

      } else {
        val fieldIdDelta = value.fieldId - previousFieldId
        if (fieldIdDelta >= 1 && fieldIdDelta <= 15) {
          // short form
          val deltaAndType = (fieldIdDelta << 4 | value.fieldTypeId).toByte
          Int8Encoder.encode(deltaAndType, buffer, writeOffset)

        } else {
          // extended form
          FieldHeaderExtendedFormEncoder.encode((value.fieldTypeId.toByte, value.fieldId.toInt), buffer, writeOffset)
        }
      }
    }
  }

  // Struct decoder

  private class StructDecoder[A](structBuilder: () => StructBuilder) extends Decoder[A] {

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
      decodeNextField(buffer, readOffset, 0, structBuilder(), 0)
    }

    private def decodeNextField(buffer: DirectBuffer, readOffset: Int, stackDepth: Int, structBuilder: StructBuilder, previousFieldId: Short): DecodeResult[A] = {
      new FieldHeaderDecoder(previousFieldId)
        .decode(buffer, readOffset)
        .andThen { (fieldHeader, buffer, readOffset) =>
          if (fieldHeader.isStopField) {
            // Done
            Decoded(structBuilder.build().asInstanceOf[A], buffer, readOffset)
          } else {
            val fieldTypeId = fieldHeader.fieldTypeId
            val fieldDecoder = (fieldTypeId: @switch) match {
              case  9 | 10 => new CollectionDecoder(structBuilder.collectionBuilderForField(fieldHeader.fieldId))
//              case 11 => new MapDecoder(structBuilder.mapBuilderForField(fieldHeader.fieldId))
              case 12 => new StructDecoder(structBuilder.structBuilderForField(fieldHeader.fieldId))
              case  _ => PrimitiveDecodersByTypeId(fieldTypeId)
            }
            fieldDecoder
              .decode(buffer, readOffset)
              .andThen { (fieldValue, buffer, readOffset) =>
                val fieldId = fieldHeader.fieldId
                val typeId = fieldHeader.fieldTypeId
                (typeId: @switch) match {
                  case 1 | 2 => structBuilder.readBoolean(fieldId, fieldValue.asInstanceOf[Boolean])
                  case 3 => structBuilder.readInt8(fieldId, fieldValue.asInstanceOf[Byte])
                  case 4 => structBuilder.readInt16(fieldId, fieldValue.asInstanceOf[Short])
                  case 5 => structBuilder.readInt32(fieldId, fieldValue.asInstanceOf[Int])
                  case 6 => structBuilder.readInt64(fieldId, fieldValue.asInstanceOf[Long])
                  case 7 => structBuilder.readDouble(fieldId, fieldValue.asInstanceOf[Double])
                  case 8 => structBuilder.readBinary(fieldId, fieldValue.asInstanceOf[Array[Byte]])
                  case 9 | 10 => structBuilder.readCollection(fieldId, fieldValue)
                  case 11 => structBuilder.readMap(fieldId, fieldValue)
                  case 12 => structBuilder.readStruct(fieldId, fieldValue)
                }

                // Initiate a bounce on the trampoline when we used up the stack-frame budget
                if (stackDepth < 100) {
                  this.decodeNextField(buffer, readOffset, stackDepth + 1, structBuilder, fieldHeader.fieldId)
                } else {
                  ContinueDecode(() => this.decodeNextField(buffer, readOffset, 0, structBuilder, fieldHeader.fieldId))
                }
              }
          }
        }
    }
  }

  private val PrimitiveDecodersByTypeId: Array[Decoder[_]] = Array(
    null,
    /* 0x1 */ BooleanFieldTrueDecoder,
    /* 0x2 */ BooleanFieldFalseDecoder,
    /* 0x3 */ Int8Decoder,
    /* 0x4 */ ZigZagInt16Decoder,
    /* 0x5 */ ZigZagInt32Decoder,
    /* 0x6 */ ZigZagInt64Decoder,
    /* 0x7 */ DoubleDecoder,
    /* 0x8 */ BinaryDecoder
    // 0x9: ListDecoder,
    // 0xA: SetDecoder,
    // 0xB: MapDecoder
    // 0xC: StructDecoder
  )

  private object StructEncoder extends Encoder[StructWriter] {

    override def encode(value: StructWriter, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      encodeNextField(value.fieldsToEncode, value, buffer, writeOffset, 0, 0)
    }

    private def encodeNextField(fieldsToEncode: Seq[StructField], structEncode: StructWriter, buffer: MutableDirectBuffer, writeOffset: Int, stackDepth: Int, previousFieldId: Short): EncodeResult = {
      if (fieldsToEncode.isEmpty) {
        // Done
        encodeStopFieldHeader(buffer, writeOffset)

      } else {
        val field = fieldsToEncode.head
        val fieldTypeId = fieldTypeIdFor(structEncode, field)
        val fieldId = field.fieldId

        new FieldHeaderEncoder(previousFieldId)
          .encode(FieldHeader(fieldId, fieldTypeId), buffer, writeOffset)
          .andThen { (buffer, writeOffset) =>

            val fieldEncodeResult: EncodeResult = (fieldTypeId: @switch) match {
              case 1 | 2 => /* value is already encoded in field header */ Encoded(buffer, writeOffset)
              case 3 => Int8Encoder.encode(structEncode.writeI8(fieldId), buffer, writeOffset)
              case 4 => Int16Encoder.encode(structEncode.writeI16(fieldId), buffer, writeOffset)
              case 5 => Int32Encoder.encode(structEncode.writeI32(fieldId), buffer, writeOffset)
              case 6 => Int64Encoder.encode(structEncode.writeI64(fieldId), buffer, writeOffset)
              case 7 => DoubleEncoder.encode(structEncode.writeDouble(fieldId), buffer, writeOffset)
              case 8 => BinaryEncoder.encode(structEncode.writeBinary(fieldId), buffer, writeOffset)
              // case 9 | 10 => CollectionEncoder.encode(structEncode.writeCollection(fieldId), buffer, writeOffset)
              // case 11 => structBuilder.readMap(fieldId, fieldValue)
              case 12 => StructEncoder.encode(structEncode.writeStruct(fieldId), buffer, writeOffset)
            }

            fieldEncodeResult.andThen { (buffer, writeOffset) =>
              // Initiate a bounce on the trampoline when we used up the stack-frame budget
              val theRest = fieldsToEncode.tail
              if (stackDepth < 100) {
                this.encodeNextField(theRest, structEncode, buffer, writeOffset, stackDepth + 1, fieldId)
              } else {
                ContinueEncode(() => this.encodeNextField(theRest, structEncode, buffer, writeOffset, 0, fieldId))
              }
            }
          }
      }
    }

    private def fieldTypeIdFor(structEncode: StructWriter, structField: StructField): Short = structField.thriftType match {
      case ThriftType.Bool => if (structEncode.writeBoolean(structField.fieldId)) 1 else 2
      case ThriftType.I8 => 3
      case ThriftType.I16 => 4
      case ThriftType.I32 => 5
      case ThriftType.I64 => 6
      case ThriftType.Double => 7
      case ThriftType.Binary => 8
      case ThriftType.List => 9
      case ThriftType.Set => 10
      case ThriftType.Map => 11
      case ThriftType.Struct => 12
    }
  }

//  private val PrimitiveEncodersByTypeId: Array[Encoder[_]] = Array(
//    null,
//    /* 0x1 */ BooleanFieldTrueEncoder,
//    /* 0x2 */ BooleanFieldFalseEncoder,
//    /* 0x3 */ Int8Encoder,
//    /* 0x4 */ ZigZagInt16Encoder,
//    /* 0x5 */ ZigZagInt32Encoder,
//    /* 0x6 */ ZigZagInt64Encoder,
//    /* 0x7 */ DoubleEncoder,
//    /* 0x8 */ BinaryEncoder
//    // 0x9: ListEncoder,
//    // 0xA: SetEncoder,
//    // 0xB: MapEncoder
//    // 0xC: StructEncoder
//  )


  // List and set decoder

  private case class ListSetHeader(size: Int, itemTypeId: Int)

  private object ListSetHeaderDecoder extends Decoder[ListSetHeader] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[ListSetHeader] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount == 0) {
        // no data available at all
        DecodeInsufficientData(this)
      } else {
        val byte1 = buffer.getByte(readOffset) & 0xFF
        // First byte looks as follows:
        //  76543210
        // +--------+
        // |sssstttt|
        // +--------+
        // s = size of the list or set (unsigned 4 bits). Value 0xF for extended form, otherwise short form.
        // t = item type id (unsigned 4 bits).
        //
        val size = byte1 >> 4
        val itemTypeId = byte1 & 0x0F

        if (isInvalidTypeId(itemTypeId)) {
          DecodeFailure("Not supported type id: " + itemTypeId)

        } else if (size != 0xF) {
          // Short form (1 byte).
          Decoded(ListSetHeader(size, itemTypeId), buffer, readOffset + 1)

        } else {
          // Extended form (5 bytes).
          //  76543210 76543210 76543210 76543210 76543210
          // +--------+--------+--------+--------+--------+
          // |1111tttt|ssssssss|ssssssss|ssssssss|ssssssss|
          // +--------+--------+--------+--------+--------+
          // t = item type id (unsigned 4 bits)
          // s = size (signed 32 bits)
          //
          // TODO: make more efficient by directly reading the size from buffer (when capacity allows it)
          //noinspection VariablePatternShadow
          Int32Decoder
            .decode(buffer, readOffset + 1)
            .andThen { case (size, buffer, readOffset) =>
              // TODO: enforce global collection size limit
              if (size < 0) {
                DecodeFailure("Collection size must be positive " + size)
              } else {
                Decoded(ListSetHeader(size, itemTypeId), buffer, readOffset)
              }
            }
        }
      }
    }
  }

  private class CollectionDecoder[A](listBuilderF: (Int) => CollectionBuilder) extends Decoder[A] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
      ListSetHeaderDecoder
        .decode(buffer, readOffset)
        .andThen { (listSetHeader, buffer, readOffset) =>
          val listBuilder = listBuilderF(listSetHeader.size)
          // TODO: replace match with array lookup:
          val itemDecoder = listSetHeader.itemTypeId match {
            case 9 | 10 => new CollectionDecoder(listBuilder.collectionBuilderForItem())
            // case 11 => new MapDecoder(listBuilder.mapBuilderForItem())
            case 12 => new StructDecoder(listBuilder.structBuilderForItem())
            case itemTypeId => PrimitiveDecodersByTypeId(itemTypeId)
          }
          decodeNextItem(buffer, readOffset, 0, listBuilder, listSetHeader.size, itemDecoder)
        }
    }

    private def decodeNextItem(buffer: DirectBuffer, readOffset: Int, stackDepth: Int, listBuilder: CollectionBuilder, itemsLeftToRead: Int, itemDecoder: Decoder[_]): DecodeResult[A] = {
      if (itemsLeftToRead <= 0) {
        // Done
        Decoded(listBuilder.build().asInstanceOf[A], buffer, readOffset)
      } else {
        itemDecoder
          .decode(buffer, readOffset)
          .andThen { (itemValue, buffer, readOffset) =>
            listBuilder.readItem(itemValue.asInstanceOf[A])

            // Initiate a bounce on the trampoline when we used up the stack-frame budget
            if (stackDepth < 50) {
              this.decodeNextItem(buffer, readOffset, stackDepth + 1, listBuilder, itemsLeftToRead - 1, itemDecoder)
            } else {
              ContinueDecode(() => this.decodeNextItem(buffer, readOffset, stackDepth + 1, listBuilder, itemsLeftToRead - 1, itemDecoder))
            }
          }
      }
    }
  }

//  private class CollectionEncoder[A] extends Encoder[A] {
//    override def encode(value: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
//
//    }
//  }

  // TODO: MapDecoder

  // Util

  private def isInvalidTypeId(typeId: Int): Boolean = typeId < 1 || typeId > 12
}
