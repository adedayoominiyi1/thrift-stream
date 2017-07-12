package nl.grons.thriftstream.decoders

import uk.co.real_logic.agrona.DirectBuffer

sealed abstract class DecodeResult[A] {
  def andThen[B](r: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B]
}

object DecodeResult {
  case class Decoded[A](value: A, buffer: DirectBuffer, nextReadOffset: Int) extends DecodeResult[A] {
    override def andThen[B](r: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] =
      r(value, buffer, nextReadOffset)
  }

  case class DecodeFailure[A](error: String) extends DecodeResult[A] {
    override def andThen[B](r: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = DecodeFailure(error)
  }

  case class DecodeInsufficientData[A](continuationDecoder: Decoder[A]) extends DecodeResult[A] {
    override def andThen[B](r: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
      // Return a decoder that can continue later, first handling the given run function
      DecodeInsufficientData(new Decoder[B] {
        override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[B] =
          continuationDecoder.decode(buffer, readOffset).andThen(r)
      })
    }
  }

  /** Trampoline continuation. See [[Decoder.trampoliningDecoder]]. */
  case class ContinueDecode[A](private val thunk: () => DecodeResult[A]) extends DecodeResult[A] {
    override def andThen[B](r: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] =
      thunk().andThen(r)
  }

}
