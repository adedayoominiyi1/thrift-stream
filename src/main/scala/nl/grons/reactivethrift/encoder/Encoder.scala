package nl.grons.reactivethrift.encoder

import uk.co.real_logic.agrona.MutableDirectBuffer

trait Encoder[A] {
  def encode(value: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult
}

object Encoder {

  def map[A, B](f: A => B, bEncoder: Encoder[B]): Encoder[A] = new Encoder[A] {
    override def encode(value: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult =
      bEncoder.encode(f(value), buffer, writeOffset)
  }

  def product[A, B](aEncoder: Encoder[A], bEncoder: Encoder[B]): Encoder[(A, B)] = new Encoder[(A, B)] {
    override def encode(ab: (A, B), buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      //noinspection VariablePatternShadow
      aEncoder
        .encode(ab._1, buffer, writeOffset)
        .andThen { case (buffer, writeOffset) => bEncoder.encode(ab._2, buffer, writeOffset) }
    }
  }

}
