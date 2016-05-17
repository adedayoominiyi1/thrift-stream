package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult._
import uk.co.real_logic.agrona.MutableDirectBuffer

/**
  * Write an i32 on the wire as a varint. The MSB of each byte is set
  * if there is another byte to follow. This can write up to 5 bytes.
  */
object VarInt32Encoder extends Encoder[Int] {
  override def encode(value: Int, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 5) {
      // Direct byte writes, the fast path
      var offset = writeOffset
      var toWrite = value
      do {
        buffer.putByte(offset, ((toWrite & 0x7f) | 0x80).toByte)
        offset += 1
        toWrite >>>= 7
      } while (toWrite != 0)
      Encoded(buffer, offset)

    } else {
      // First create byte array, the slow path
      val bytes = Array.ofDim[Byte](5)
      var varIntByteCount = 0
      var toWrite = value
      do {
        bytes(varIntByteCount) = ((toWrite & 0x7f) | 0x80).toByte
        varIntByteCount += 1
        toWrite >>>= 7
      } while (toWrite != 0)
      val varIntBytes = bytes.take(varIntByteCount)
      BytesEncoder.encode(varIntBytes, buffer, writeOffset)
    }
  }
}

/**
  * Write an i64 on the wire as a varint. The MSB of each byte is set
  * if there is another byte to follow. This can write up to 10 bytes.
  */
object VarInt64Encoder extends Encoder[Long] {
  override def encode(value: Long, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount >= 10) {
      // Direct byte writes, the fast path
      var offset = writeOffset
      var toWrite = value
      do {
        buffer.putByte(offset, ((toWrite & 0x7f) | 0x80).toByte)
        offset += 1
        toWrite >>>= 7
      } while (toWrite != 0)
      Encoded(buffer, offset)

    } else {
      // First create byte array, the slow path
      val bytes = Array.ofDim[Byte](10)
      var varIntByteCount = 0
      var toWrite = value
      do {
        bytes(varIntByteCount) = ((toWrite & 0x7fL) | 0x80).toByte
        varIntByteCount += 1
        toWrite >>>= 7
      } while (toWrite != 0)
      val varIntBytes = bytes.take(varIntByteCount)
      BytesEncoder.encode(varIntBytes, buffer, writeOffset)
    }
  }
}

