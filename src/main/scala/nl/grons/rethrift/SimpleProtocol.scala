package nl.grons.rethrift

import uk.co.real_logic.agrona.DirectBuffer
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

object SimpleProtocol extends Protocol {

  override def structDecoder[A](structBuilder: () => StructBuilder): Decoder[A] = {
    Decoder.trampoliningDecoder(new StructDecoder(structBuilder))
  }

  // Simple decoders

  object BooleanTrueDecoder extends Decoder[Boolean] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Boolean] = Decoded(true, buffer, readOffset)
  }

  object BooleanFalseDecoder extends Decoder[Boolean] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Boolean] = Decoded(false, buffer, readOffset)
  }

  object Int8Decoder extends Decoder[Byte] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Byte] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 1) {
        val value = buffer.getByte(readOffset)
        Decoded(value, buffer, readOffset + 1)
      } else {
        DecodeUnsufficientData(this)
      }
    }
  }

  object Int16Decoder extends Decoder[Short] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 2) {
        val value = buffer.getShort(readOffset)
        Decoded(value, buffer, readOffset + 2)
      } else {
        val availableBytes = Array.ofDim[Byte](availableByteCount)
        buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
        DecodeUnsufficientData(new Int16ContinuationDecoder(availableBytes))
      }
    }

    private class Int16ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Short] {
      require(previousData.length < 2)

      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
        val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 2)
        if (concatenatedData.length == 2) {
          val value = new UnsafeBuffer(concatenatedData).getShort(0)
          Decoded(value, buffer, newDataReadOffset)
        } else {
          DecodeUnsufficientData(new Int16ContinuationDecoder(concatenatedData))
        }
      }
    }
  }

  object Int32Decoder extends Decoder[Int] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 4) {
        val value = buffer.getInt(readOffset)
        Decoded(value, buffer, readOffset + 4)
      } else {
        val availableBytes = Array.ofDim[Byte](availableByteCount)
        buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
        DecodeUnsufficientData(new Int32ContinuationDecoder(availableBytes))
      }
    }

    private class Int32ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Int] {
      require(previousData.length < 4)

      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
        val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 4)
        if (concatenatedData.length == 4) {
          val value = new UnsafeBuffer(concatenatedData).getInt(0)
          Decoded(value, buffer, newDataReadOffset)
        } else {
          DecodeUnsufficientData(new Int32ContinuationDecoder(concatenatedData))
        }
      }
    }
  }

  object Int64Decoder extends Decoder[Long] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 8) {
        val value = buffer.getLong(readOffset)
        Decoded(value, buffer, readOffset + 8)
      } else {
        val availableBytes = Array.ofDim[Byte](availableByteCount)
        buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
        DecodeUnsufficientData(new Int64ContinuationDecoder(availableBytes))
      }
    }

    private class Int64ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Long] {
      require(previousData.length < 8)

      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
        val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 8)
        if (concatenatedData.length == 8) {
          val value = new UnsafeBuffer(concatenatedData).getLong(0)
          Decoded(value, buffer, newDataReadOffset)
        } else {
          DecodeUnsufficientData(new Int64ContinuationDecoder(concatenatedData))
        }
      }
    }
  }

  case class FieldHeader(fieldId: Short, fieldTypeId: Int) {
    def isStopField = fieldTypeId == 0
  }

  class FieldHeaderDecoder(previousFieldId: Short) extends Decoder[FieldHeader] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[FieldHeader] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount >= 1) {
        val byte1 = buffer.getByte(readOffset) & 0xff
        // First byte looks as follows:
        //  76543210
        // +--------+
        // |ddddtttt|
        // +--------+
        // d = field id delta (unsigned 4 bits), >0, for the short form, 0 for stop field and extended form
        // t = field type id (4 bits), 0 for stop field
        //
        if (byte1 == 0) {
          Decoded(FieldHeader(0, 0), buffer, readOffset + 1)

        } else {
          val fieldIdDelta = (byte1 >> 4).toShort
          val fieldTypeId = byte1 & 0x0F

          if (DecoderUtil.isInvalidTypeId(fieldTypeId)) {
            DecodeFailure("Not supported type id: " + fieldTypeId)

          } else if (fieldIdDelta != 0) {
            // Short form (1 byte).
            val fieldId = (previousFieldId + fieldIdDelta).toShort
            Decoded(FieldHeader(fieldId, fieldTypeId), buffer, readOffset + 1)

          } else {
            // Extended form (3 bytes).
            //  76543210 76543210 76543210
            // +--------+--------+--------+
            // |0000tttt|iiiiiiii|iiiiiiii|
            // +--------+--------+--------+
            // t = field type id (4 bits)
            // i = field id (signed 16 bits)
            //
            if (availableByteCount >= 3) {
              val fieldId = buffer.getShort(readOffset + 1)
              Decoded(FieldHeader(fieldId, fieldTypeId), buffer, readOffset + 3)

            } else {
              val availableBytes = Array.ofDim[Byte](availableByteCount)
              buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
              DecodeUnsufficientData(new FieldHeaderContinuationDecoder(availableBytes))
            }
          }
        }
      } else {
        // no data available at all
        DecodeUnsufficientData(this)
      }
    }
  }

  class FieldHeaderContinuationDecoder(previousData: Array[Byte]) extends Decoder[FieldHeader] {
    require(previousData.length < 3)

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[FieldHeader] = {
      val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 3)
      if (concatenatedData.length == 3) {
        val concatenatedBuffer = new UnsafeBuffer(concatenatedData)
        val fieldTypeId = concatenatedBuffer.getByte(0) & 0x0f
        val fieldId = concatenatedBuffer.getShort(1)
        if (DecoderUtil.isInvalidTypeId(fieldTypeId)) {
          DecodeFailure("Not supported field type id: " + fieldTypeId)
        } else {
          Decoded(FieldHeader(fieldId, fieldTypeId), buffer, newDataReadOffset)
        }
      } else {
        DecodeUnsufficientData(new FieldHeaderContinuationDecoder(concatenatedData))
      }
    }
  }

  // Struct decoder

  class StructDecoder[A](structBuilder: () => StructBuilder) extends Decoder[A] {

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
            val fieldDecoder = fieldTypeId match {
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
                typeId match {
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

                // Initiate a bounce on the trampoline when we reached our stack-frames budget
                if (stackDepth < 100) {
                  this.decodeNextField(buffer, readOffset, stackDepth + 1, structBuilder, fieldHeader.fieldId)
                } else {
                  Continue(() => this.decodeNextField(buffer, readOffset, 0, structBuilder, fieldHeader.fieldId))
                }
              }
          }
        }
    }
  }

  // TODO: implement Double and Array[Byte] decoders
  private def notImplemented[A]: Decoder[A] = new Decoder[A] {
    def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = ???
  }

  private val PrimitiveDecodersByTypeId: Array[Decoder[_]] = Array(
    null,
    /* 0x1 */ BooleanTrueDecoder,
    /* 0x2 */ BooleanFalseDecoder,
    /* 0x3 */ Int8Decoder,
    /* 0x4 */ Int16Decoder,
    /* 0x5 */ Int32Decoder,
    /* 0x6 */ Int64Decoder,
    /* 0x7 */ notImplemented[Double], // DoubleDecoder,
    /* 0x8 */ notImplemented[Array[Byte]] // BinaryDecoder
    // 0x9: ListDecoder,
    // 0xA: SetDecoder,
    // 0xB: MapDecoder
    // 0xC: StructDecoder
  )

  // List and set decoder

  case class ListSetHeader(size: Int, itemTypeId: Int)

  object ListSetHeaderDecoder extends Decoder[ListSetHeader] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[ListSetHeader] = {
      val availableByteCount = buffer.capacity() - readOffset
      if (availableByteCount < 1) {
        // no data available at all
        DecodeUnsufficientData(this)
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

        if (itemTypeId < 1 || itemTypeId > 12) {
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
          if (availableByteCount >= 5) {
            val size = buffer.getInt(readOffset + 1)
            if (size < 0) {
              DecodeFailure("Collection size must be positive " + size)
            } else {
              Decoded(ListSetHeader(size, itemTypeId), buffer, readOffset + 5)
            }

          } else {
            val availableBytes = Array.ofDim[Byte](availableByteCount)
            buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
            DecodeUnsufficientData(new ListSetHeaderContinuationDecoder(availableBytes))
          }
        }
      }
    }

    private class ListSetHeaderContinuationDecoder(previousData: Array[Byte]) extends Decoder[ListSetHeader] {
      require(previousData.length < 5)

      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[ListSetHeader] = {
        val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 5)
        if (concatenatedData.length == 5) {
          val concatenatedBuffer = new UnsafeBuffer(concatenatedData)
          val itemTypeId = concatenatedBuffer.getByte(0) & 0x0f
          val size = concatenatedBuffer.getInt(1)
          // itemTypeId was already verified to be correct
          if (size < 0) {
            DecodeFailure("Collection size must be positive " + size)
          } else {
            Decoded(ListSetHeader(size, itemTypeId), buffer, newDataReadOffset)
          }
        } else {
          DecodeUnsufficientData(new ListSetHeaderContinuationDecoder(concatenatedData))
        }
      }
    }
  }

  class CollectionDecoder[A](listBuilderF: (Int) => CollectionBuilder) extends Decoder[A] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
      ListSetHeaderDecoder
        .decode(buffer, readOffset)
        .andThen { (listSetHeader, buffer, readOffset) =>
          // TODO: enforce global collection size limit
          val listBuilder = listBuilderF(listSetHeader.size)
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

            // Initiate a bounce on the trampoline when we reached our stack-frames budget
            if (stackDepth < 50) {
              this.decodeNextItem(buffer, readOffset, stackDepth + 1, listBuilder, itemsLeftToRead - 1, itemDecoder)
            } else {
              Continue(() => this.decodeNextItem(buffer, readOffset, stackDepth + 1, listBuilder, itemsLeftToRead - 1, itemDecoder))
            }
          }
      }
    }
  }

  // Util

  private object DecoderUtil {
    def isInvalidTypeId(typeId: Int): Boolean = typeId < 1 || typeId > 12

    /**
      * Concatenates the bytes in 'previousData' with bytes from 'newData' such that the result is 'needed' bytes
      * long (if possible).
      *
      * @param previousData the previously collected bytes
      * @param newData      the new data to concatenate to previousData
      * @param readOffset   the read offset in newData
      * @param needed       the desired size of the result
      * @return the concatenated data (size is between `previousData.length` and 'needed' bytes, both inclusive),
      *         and the new 'readOffset' in newData
      */
    def concatData(previousData: Array[Byte], newData: DirectBuffer, readOffset: Int, needed: Int): (Array[Byte], Int) = {
      val resultSize = Math.min(needed, previousData.length + newData.capacity() - readOffset)
      val concatenatedData = Array.ofDim[Byte](resultSize)
      System.arraycopy(previousData, 0, concatenatedData, 0, previousData.length)
      val bytesFromNewData = resultSize - previousData.length
      newData.getBytes(readOffset, concatenatedData, previousData.length, bytesFromNewData)
      (concatenatedData, readOffset + bytesFromNewData)
    }
  }

}