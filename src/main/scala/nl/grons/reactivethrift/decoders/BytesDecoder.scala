package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer
import nl.grons.reactivethrift.decoders.BytesDecoder._

/**
  * Bytes encoder.
  *
  * Protocol: just the bytes (no length prefix).
  * @param length expected number of bytes
  */
case class BytesDecoder(length: Int) extends Decoder[Array[Byte]] {
  require(length >= 0)

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Array[Byte]] = {
    val value = Array.ofDim[Byte](length)
    doDecode(length, 0, value, buffer, readOffset)
  }
}

object BytesDecoder {
  private class BinaryContinuationDecoder(length: Int, readCount: Int, value: Array[Byte]) extends Decoder[Array[Byte]] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Array[Byte]] =
      doDecode(length, readCount, value, buffer, readOffset)
  }

  private def doDecode(length: Int, readCount: Int, data: Array[Byte], buffer: DirectBuffer, readOffset: Int): DecodeResult[Array[Byte]] with Product with Serializable = {
    val availableByteCount = buffer.capacity() - readOffset
    val copyCount = Math.min(length - readCount, availableByteCount)
    buffer.getBytes(readOffset, data, readCount, copyCount)
    val newReadCount = readCount + copyCount
    if (newReadCount == length) {
      Decoded(data, buffer, readOffset + copyCount)
    } else {
      DecodeInsufficientData(new BinaryContinuationDecoder(length, newReadCount, data))
    }
  }
}