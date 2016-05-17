package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult._
import uk.co.real_logic.agrona.MutableDirectBuffer

/**
  * Bytes encoder.
  *
  * Protocol: just the bytes, byte with index 0 first.
  */
object BytesEncoder extends Encoder[Array[Byte]] {

  override def encode(value: Array[Byte], buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
    val availableByteCount = buffer.capacity() - writeOffset
    val writeCount = Math.min(value.length, availableByteCount)
    buffer.putBytes(writeOffset, value, 0, writeCount)
    if (writeCount == value.length) {
      Encoded(buffer, writeOffset + writeCount)
    } else {
      EncodeInsufficientBuffer(new ByteArrayContinuationEncoder(value, writeCount))
    }
  }

  private class ByteArrayContinuationEncoder(value: Array[Byte], written: Int) extends ContinuationEncoder {
    override def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      val availableByteCount = buffer.capacity() - writeOffset
      val writeCount = Math.min(value.length - written, availableByteCount)
      buffer.putBytes(writeOffset, value, written, writeCount)
      val newWritten = written + writeCount
      if (newWritten == value.length) {
        Encoded(buffer, writeOffset + writeCount)
      } else {
        EncodeInsufficientBuffer(new ByteArrayContinuationEncoder(value, newWritten))
      }
    }
  }
}
