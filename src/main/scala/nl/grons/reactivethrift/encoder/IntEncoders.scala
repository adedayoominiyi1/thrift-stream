package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult._
import uk.co.real_logic.agrona.MutableDirectBuffer

/**
  * Int8 (Byte) encoder.
  *
  * Protocol: just the byte.
  */
object Int8Encoder extends Encoder[Byte] {
  override def encode(value: Byte, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount == 0) {
      EncodeInsufficientBuffer(new Int8ContinuationEncoder(value))
    } else {
      buffer.putByte(writeOffset, value)
      Encoded(buffer, writeOffset + 1)
    }
  }

  private class Int8ContinuationEncoder(value: Byte) extends ContinuationEncoder {
    override def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult =
      Int8Encoder.encode(value, buffer, writeOffset)
  }
}

/**
  * Int16 (Short) encoder.
  *
  * Protocol: 2 bytes, most significant byte first.
  */
object Int16Encoder extends Encoder[Short] {
  override def encode(value: Short, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 2) {
      buffer.putShort(writeOffset, value)
      Encoded(buffer, writeOffset + 2)
    } else {
      val bytes = Array[Byte](
        ((value >> 8) & 0xff).toByte,
        (value         & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}

/**
  * Int32 (Int) encoder.
  *
  * Protocol: 4 bytes, most significant byte first.
  */
object Int32Encoder extends Encoder[Int] {
  override def encode(value: Int, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 4) {
      buffer.putInt(writeOffset, value)
      Encoded(buffer, writeOffset + 4)
    } else {
      val bytes = Array[Byte](
        ((value >> 24) & 0xff).toByte,
        ((value >> 16) & 0xff).toByte,
        ((value >>  8) & 0xff).toByte,
        (value         & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}

/**
  * Int64 (Long) encoder.
  *
  * Protocol: 8 bytes, most significant byte first.
  */
object Int64Encoder extends Encoder[Long] {
  override def encode(value: Long, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 8) {
      buffer.putLong(writeOffset, value)
      Encoded(buffer, writeOffset + 8)
    } else {
      val bytes = Array[Byte](
        ((value >> 54) & 0xff).toByte,
        ((value >> 48) & 0xff).toByte,
        ((value >> 40) & 0xff).toByte,
        ((value >> 32) & 0xff).toByte,
        ((value >> 24) & 0xff).toByte,
        ((value >> 16) & 0xff).toByte,
        ((value >>  8) & 0xff).toByte,
        (value         & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}
