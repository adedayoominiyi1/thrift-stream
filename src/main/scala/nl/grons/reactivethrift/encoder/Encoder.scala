package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult.{EncodeInsufficientBuffer, Encoded}
import uk.co.real_logic.agrona.MutableDirectBuffer

trait Encoder[A] {
  def encode(a: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult
}

trait ContinuationEncoder {
  def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult
}

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
    override def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      val availableByteCount = buffer.capacity() - writeOffset
      if (availableByteCount == 0) {
        EncodeInsufficientBuffer(this)
      } else {
        buffer.putByte(writeOffset, value)
        Encoded(buffer, writeOffset + 1)
      }
    }
  }
}
