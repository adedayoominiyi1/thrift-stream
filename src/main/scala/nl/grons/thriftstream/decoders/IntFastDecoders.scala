package nl.grons.thriftstream.decoders

import nl.grons.thriftstream.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer

/**
  * Int16 decoder.
  *
  * Protocol: 2 bytes, least significant byte first.
  */
object Int16FastDecoder extends Decoder[Short] {
  private val BytesToShortDecoder = BytesDecoder(2)
    .map(bytes => ((bytes(0) & 0xff) | (bytes(1) & 0xff) << 8).toShort)

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 2) {
      val value = buffer.getShort(readOffset)
      Decoded(value, buffer, readOffset + 2)
    } else {
      BytesToShortDecoder.decode(buffer, readOffset)
    }
  }
}

/**
  * Int32 decoder.
  *
  * Protocol: 4 bytes, least significant byte first.
  */
object Int32FastDecoder extends Decoder[Int] {
  val BytesToShortDecoder = BytesDecoder(4)
    .map { bytes =>
      (bytes(0) & 0xff) |
      (bytes(1) & 0xff) << 8 |
      (bytes(2) & 0xff) << 16 |
      (bytes(3) & 0xff) << 24
    }

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 4) {
      val value = buffer.getInt(readOffset)
      Decoded(value, buffer, readOffset + 4)
    } else {
      BytesToShortDecoder.decode(buffer, readOffset)
    }
  }
}

/**
  * Int64 decoder.
  *
  * Protocol: 8 bytes, least significant byte first.
  */
object Int64FastDecoder extends Decoder[Long] {
  val BytesToLongDecoder = BytesDecoder(8)
    .map(bytes =>
      (bytes(0) & 0xffL) |
      (bytes(1) & 0xffL) <<  8 |
      (bytes(2) & 0xffL) << 16 |
      (bytes(3) & 0xffL) << 24 |
      (bytes(4) & 0xffL) << 32 |
      (bytes(5) & 0xffL) << 40 |
      (bytes(6) & 0xffL) << 48 |
      (bytes(7) & 0xffL) << 56
    )

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 8) {
      val value = buffer.getLong(readOffset)
      Decoded(value, buffer, readOffset + 8)
    } else {
      BytesToLongDecoder.decode(buffer, readOffset)
    }
  }
}
