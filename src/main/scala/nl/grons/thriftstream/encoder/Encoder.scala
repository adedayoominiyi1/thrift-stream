package nl.grons.thriftstream.encoder

import nl.grons.thriftstream.encoder.EncodeResult.Encoded
import uk.co.real_logic.agrona.MutableDirectBuffer

trait Encoder[A] { self =>
  def encode(value: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult

  /**
    * Wrap this encoder in a new encoder that first applies `f` to the value to be encoded.
    */
  def map[B](f: B => A): Encoder[B] = new Encoder[B] {
    override def encode(value: B, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult =
      self.encode(f(value), buffer, writeOffset)
  }
}

object Encoder {

  /**
    * An encoder that does nothing and always succeeds.
    */
  def zero[A]: Encoder[A] = new Encoder[A] {
    override def encode(value: A, buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult =
      Encoded(buffer, writeOffset)
  }

  /**
    * Wrap `bEncoder` in a new encoder that first applies `f` to the value to be encoded.
    */
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

  def product4[A, B, C, D](aEncoder: Encoder[A], bEncoder: Encoder[B], cEncoder: Encoder[C], dEncoder: Encoder[D]): Encoder[(A, B, C, D)] = new Encoder[(A, B, C, D)] {
    override def encode(value: (A, B, C, D), buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult = {
      //noinspection VariablePatternShadow
      aEncoder
        .encode(value._1, buffer, writeOffset)
        .andThen { case (buffer, writeOffset) =>
          bEncoder
            .encode(value._2, buffer, writeOffset)
            .andThen { case (buffer, writeOffset) =>
              cEncoder
                .encode(value._3, buffer, writeOffset)
                .andThen { case (buffer, writeOffset) =>
                  dEncoder
                    .encode(value._4, buffer, writeOffset)
                }
            }
        }
    }
  }

}
