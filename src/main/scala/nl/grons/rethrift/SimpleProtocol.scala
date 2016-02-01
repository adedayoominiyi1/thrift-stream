package nl.grons.rethrift

import uk.co.real_logic.agrona.DirectBuffer
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

import scala.annotation.tailrec

object SimpleProtocol extends Protocol {

  def structDecoder[A](structBuilder: StructBuilder[A]): Decoder[A] = {
    new StructDecoder[A](structBuilder)
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

          if (fieldTypeId < 1 || fieldTypeId > 12) {
            DecodeFailure("Not supported field type id: " + fieldTypeId)

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
        if (fieldTypeId < 1 || fieldTypeId > 12) {
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

  class StructDecoder[A](structBuilder: StructBuilder[A]) extends Decoder[A] {
    var previousFieldId: Short = 0

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
      new FieldHeaderDecoder(previousFieldId)
        .decode(buffer, readOffset)
        .andThen { (fieldHeader, buffer, readOffset) =>
          if (fieldHeader.isStopField) {
            Decoded(structBuilder.stopStruct(), buffer, readOffset)
          } else {
            val fieldTypeId = fieldHeader.fieldTypeId
            val fieldDecoder = if (fieldTypeId == 12) {
              new StructDecoder(structBuilder.startStruct(fieldHeader.fieldId))
            } else {
              StructCoder.DecodersByTypeId(fieldTypeId)
            }
            fieldDecoder.decode(buffer, readOffset)
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
                  case 12 => structBuilder.readStruct(fieldId, fieldValue)
                  case _ => ??? // TODO: add support for list, set, map
                }
                this.previousFieldId = fieldHeader.fieldId
                // TODO: prevent huge stack usage (this code uses 2 stackframes per field)
                this.decode(buffer, readOffset)
              }
          }
        }
    }
  }

  object StructCoder {
    val DecodersByTypeId: Array[Decoder[_]] = Array(
      null,
      /* 0x1 */ BooleanTrueDecoder,
      /* 0x2 */ BooleanFalseDecoder,
      /* 0x3 */ Int8Decoder,
      /* 0x4 */ Int16Decoder,
      /* 0x5 */ Int32Decoder,
      /* 0x6 */ Int64Decoder,
      /* 0x7 */ ???, // DoubleDecoder,
      /* 0x8 */ ???, // BinaryDecoder,
      /* 0x9 */ ???, // ListDecoder,
      /* 0xA */ ???, // SetDecoder,
      /* 0xB */ ??? // MapDecoder
      // 0xC: StructDecoder
    )
  }

  object DecoderUtil {
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