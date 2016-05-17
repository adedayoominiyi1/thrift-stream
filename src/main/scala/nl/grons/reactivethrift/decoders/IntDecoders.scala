package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer

/**
  * Int8 decoder.
  * Protocol: just 1 byte.
  */
object Int8Decoder extends Decoder[Byte] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Byte] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 1) {
      val value = buffer.getByte(readOffset)
      Decoded(value, buffer, readOffset + 1)
    } else {
      DecodeInsufficientData(this)
    }
  }
}

/**
  * Int16 decoder.
  * Protocol: 2 bytes, most significant byte first.
  */
object Int16Decoder extends Decoder[Short] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 2) {
      val value = buffer.getShort(readOffset)
      Decoded(value, buffer, readOffset + 2)
    } else {
      BytesDecoder(2)
        .map(bytes => (bytes(0) << 8 | bytes(1)).toShort)
        .decode(buffer, readOffset)
    }
  }
}

/**
  * Int32 decoder.
  * Protocol: 4 bytes, most significant byte first.
  */
object Int32Decoder extends Decoder[Int] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 4) {
      val value = buffer.getInt(readOffset)
      Decoded(value, buffer, readOffset + 4)
    } else {
      BytesDecoder(4)
        .map(bytes => bytes(0) << 24 | bytes(1) << 16 | bytes(2) << 8 | bytes(3))
        .decode(buffer, readOffset)
    }
  }
}

/**
  * Int64 decoder.
  * Protocol: 8 bytes, most significant byte first.
  */
object Int64Decoder extends Decoder[Long] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 8) {
      val value = buffer.getLong(readOffset)
      Decoded(value, buffer, readOffset + 8)
    } else {
      BytesDecoder(8)
        .map(bytes =>
          bytes(0) << 56L | bytes(1) << 48L | bytes(2) << 40L | bytes(3) << 32L |
          bytes(4) << 24L | bytes(5) << 16L | bytes(6) <<  8L | bytes(7).toLong
        )
        .decode(buffer, readOffset)
    }
  }
}
