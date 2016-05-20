package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer

/**
  * Int8 decoder.
  *
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
  *
  * Protocol: 2 bytes, most significant byte first.
  */
object Int16Decoder extends Decoder[Short] {
  private val BytesToShortDecoder = BytesDecoder(2)
    .map(bytes => ((bytes(0) & 0xff) << 8 | (bytes(1) & 0xff)).toShort)

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 2) {
      val value = ((buffer.getByte(readOffset) & 0xff) << 8 | (buffer.getByte(readOffset + 1) & 0xff)).toShort
      Decoded(value, buffer, readOffset + 2)
    } else {
      BytesToShortDecoder.decode(buffer, readOffset)
    }
  }
}

/**
  * Int32 decoder.
  *
  * Protocol: 4 bytes, most significant byte first.
  */
object Int32Decoder extends Decoder[Int] {
  private val BytesToIntDecoder = BytesDecoder(4)
    .map { bytes =>
      (bytes(0) & 0xff) << 24 |
      (bytes(1) & 0xff) << 16 |
      (bytes(2) & 0xff) << 8 |
      (bytes(3) & 0xff)
    }

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 4) {
      val value =
        (buffer.getByte(readOffset    ) & 0xff) << 24 |
        (buffer.getByte(readOffset + 1) & 0xff) << 16 |
        (buffer.getByte(readOffset + 2) & 0xff) << 8 |
        (buffer.getByte(readOffset + 3) & 0xff)
      Decoded(value, buffer, readOffset + 4)
    } else {
      BytesToIntDecoder.decode(buffer, readOffset)
    }
  }
}

/**
  * Int64 decoder.
  *
  * Protocol: 8 bytes, most significant byte first.
  */
object Int64Decoder extends Decoder[Long] {
  private val BytesToLongDecoder = BytesDecoder(8)
    .map(bytes =>
      (bytes(0) & 0xffL) << 56 |
      (bytes(1) & 0xffL) << 48 |
      (bytes(2) & 0xffL) << 40 |
      (bytes(3) & 0xffL) << 32 |
      (bytes(4) & 0xffL) << 24 |
      (bytes(5) & 0xffL) << 16 |
      (bytes(6) & 0xffL) <<  8 |
      (bytes(7) & 0xffL)
    )

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 8) {
      val value =
        (buffer.getByte(readOffset    ) & 0xffL) << 56 |
        (buffer.getByte(readOffset + 1) & 0xffL) << 48 |
        (buffer.getByte(readOffset + 2) & 0xffL) << 40 |
        (buffer.getByte(readOffset + 3) & 0xffL) << 32 |
        (buffer.getByte(readOffset + 4) & 0xffL) << 24 |
        (buffer.getByte(readOffset + 5) & 0xffL) << 16 |
        (buffer.getByte(readOffset + 6) & 0xffL) <<  8 |
        (buffer.getByte(readOffset + 7) & 0xffL)
      Decoded(value, buffer, readOffset + 8)
    } else {
      BytesToLongDecoder.decode(buffer, readOffset)
    }
  }
}
