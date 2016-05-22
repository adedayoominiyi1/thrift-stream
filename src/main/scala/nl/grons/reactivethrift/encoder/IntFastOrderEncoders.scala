package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult._
import uk.co.real_logic.agrona.MutableDirectBuffer

/**
  * Int16 (Short) encoder.
  *
  * Protocol: 2 bytes, least significant byte first (little endian).
  */
object Int16FastEncoder extends Encoder[Short] {
  override def encode(value: Short, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 2) {
      buffer.putShort(writeOffset, value)
      Encoded(buffer, writeOffset + 2)
    } else {
      val bytes = Array[Byte](
        (value        & 0xff).toByte,
        ((value >> 8) & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}

/**
  * Int32 (Int) encoder.
  *
  * Protocol: 4 bytes, least significant byte first (little endian).
  */
object Int32FastEncoder extends Encoder[Int] {
  override def encode(value: Int, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 4) {
      buffer.putInt(writeOffset, value)
      Encoded(buffer, writeOffset + 4)
    } else {
      val bytes = Array[Byte](
        (value         & 0xff).toByte,
        ((value >>  8) & 0xff).toByte,
        ((value >> 16) & 0xff).toByte,
        ((value >> 24) & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}

/**
  * Int64 (Long) encoder.
  *
  * Protocol: 8 bytes, least significant byte first (little endian).
  */
object Int64FastEncoder extends Encoder[Long] {
  override def encode(value: Long, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 8) {
      buffer.putLong(writeOffset, value)
      Encoded(buffer, writeOffset + 8)
    } else {
      val bytes = Array[Byte](
        (value         & 0xff).toByte,
        ((value >>  8) & 0xff).toByte,
        ((value >> 16) & 0xff).toByte,
        ((value >> 24) & 0xff).toByte,
        ((value >> 32) & 0xff).toByte,
        ((value >> 40) & 0xff).toByte,
        ((value >> 48) & 0xff).toByte,
        ((value >> 56) & 0xff).toByte
      )
      BytesEncoder.encode(bytes, buffer, writeOffset)
    }
  }
}
