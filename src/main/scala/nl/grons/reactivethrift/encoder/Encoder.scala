package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.encoder.EncodeResult.{EncodeInsufficientBuffer, Encoded}
import uk.co.real_logic.agrona.MutableDirectBuffer

trait Encoder[A] {
  val value: A
  def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult[A]

  def flatMap[B](f: A => Encoder[B]): Encoder[B] = f(value)
}

object Encoder {

}

case class Int8Encoder(value: Byte) extends Encoder[Byte] {
  override def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult[Byte] = {
    val availableByteCount = buffer.capacity() - writeOffset
    if (availableByteCount > 0) {
      buffer.putByte(writeOffset, value)
      Encoded(buffer, writeOffset + 1)
    } else {
      EncodeInsufficientBuffer(this)
    }
  }
}
