package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer

import scala.annotation.tailrec

trait Decoder[A] { self =>
  def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A]

  /**
    * Creates a new decoder by applying function `f` to the decode result of this decoder.
    */
  def map[B](f: A => B): Decoder[B] = new Decoder[B] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[B] = {
      //noinspection VariablePatternShadow
      self
        .decode(buffer, readOffset)
        .andThen { case (a, buffer, readOffset) =>
          Decoded(f(a), buffer, readOffset)
        }
    }
  }

  /**
    * Creates a new decoder that calls the decoder created by `f` applied to the result of this decoder.
    */
  def flatMap[B](f: A => Decoder[B]): Decoder[B] = new Decoder[B] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[B] = {
      //noinspection VariablePatternShadow
      self
        .decode(buffer, readOffset)
        .andThen { case (a, buffer, readOffset) =>
          f(a).decode(buffer, readOffset)
        }
    }
  }

  def trampolined: Decoder[A] = Decoder.trampoliningDecoder(self)

  def product[B](bDecoder: Decoder[B]): Decoder[(A, B)] = Decoder.product(self, bDecoder)
}

object Decoder {

  /**
    * A [[Decoder]] that always returns [[Decoded]] with the given value `a`.
    */
  def unit[A](a: A): Decoder[A] = new Decoder[A] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = Decoded(a, buffer, readOffset)
  }

  /**
    * A [[Decoder]] that only returns [[Decoded]], [[DecodeFailure]] or [[DecodeInsufficientData]]
    * (and ''not'' [[Continue]]) results.
    * It does this by replacing a [[Continue]] with the result of executing its embedded thunk.
    *
    * See [[http://blog.richdougherty.com/2009/04/tail-calls-tailrec-and-trampolines.html]] for inspiration.
    *
    * @param decoder the decoder to wrap
    * @tparam A type of expected decode result
    * @return a new decoder
    */
  def trampoliningDecoder[A](decoder: Decoder[A]): Decoder[A] = {
    @tailrec
    def trampoline(decodeResult: DecodeResult[A]): DecodeResult[A] = {
      decodeResult match {
        case Continue(thunk) => trampoline(thunk())
        case result => result
      }
    }

    new Decoder[A] {
      def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
        trampoline(decoder.decode(buffer, readOffset))
      }
    }
  }

  def product[A, B](aDecoder: Decoder[A], bDecoder: Decoder[B]): Decoder[(A, B)] = new Decoder[(A, B)] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[(A, B)] = {
      //noinspection VariablePatternShadow
      aDecoder
        .decode(buffer, readOffset)
        .andThen { case (a, buffer, readOffset) =>
          bDecoder
            .decode(buffer, readOffset)
            .andThen { case (b, buffer, readOffset) =>
              Decoded((a, b), buffer, readOffset)
            }
        }
    }
  }

  def product3[A, B, C](aDecoder: Decoder[A], bDecoder: Decoder[B], cDecoder: Decoder[C]): Decoder[(A, B, C)] = new Decoder[(A, B, C)] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[(A, B, C)] = {
      //noinspection VariablePatternShadow
      aDecoder
        .decode(buffer, readOffset)
        .andThen { case (a, buffer, readOffset) =>
          bDecoder
            .decode(buffer, readOffset)
            .andThen { case (b, buffer, readOffset) =>
              cDecoder
                .decode(buffer, readOffset)
                .andThen { case (c, buffer, readOffset) =>
                  Decoded((a, b, c), buffer, readOffset)
                }
            }
        }
    }
  }

  def product4[A, B, C, D](aDecoder: Decoder[A], bDecoder: Decoder[B], cDecoder: Decoder[C], dDecoder: Decoder[D]): Decoder[(A, B, C, D)] = new Decoder[(A, B, C, D)] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[(A, B, C, D)] = {
      //noinspection VariablePatternShadow
      aDecoder
        .decode(buffer, readOffset)
        .andThen { case (a, buffer, readOffset) =>
          bDecoder
            .decode(buffer, readOffset)
            .andThen { case (b, buffer, readOffset) =>
              cDecoder
                .decode(buffer, readOffset)
                .andThen { case (c, buffer, readOffset) =>
                  dDecoder
                    .decode(buffer, readOffset)
                    .andThen { case (d, buffer, readOffset) =>
                      Decoded((a, b, c, d), buffer, readOffset)
                    }
                }
            }
        }
    }
  }
}
