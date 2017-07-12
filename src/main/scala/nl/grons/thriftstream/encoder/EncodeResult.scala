package nl.grons.thriftstream.encoder

import uk.co.real_logic.agrona.{DirectBuffer, MutableDirectBuffer}

sealed abstract class EncodeResult {
  def andThen(r: (MutableDirectBuffer, Int) => EncodeResult): EncodeResult
}

object EncodeResult {

  case class Encoded(buffer: MutableDirectBuffer, nextWriteOffset: Int) extends EncodeResult {
    override def andThen(r: (MutableDirectBuffer, Int) => EncodeResult): EncodeResult =
      r(buffer, nextWriteOffset)
  }

  case class EncodeFailure(error: String) extends EncodeResult {
    override def andThen(r: (MutableDirectBuffer, Int) => EncodeResult): EncodeResult = EncodeFailure(error)
  }

  /** Trampoline continuation. See [[Encoder.trampoliningEncoder]]. */
  case class ContinueEncode(private val thunk: () => EncodeResult) extends EncodeResult {
    override def andThen(r: (MutableDirectBuffer, Int) => EncodeResult): EncodeResult =
      thunk().andThen(r)
  }

  trait ContinuationEncoder {
    def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult
  }

  case class EncodeInsufficientBuffer(continuationEncoder: ContinuationEncoder) extends EncodeResult {
    override def andThen(r: (MutableDirectBuffer, Int) => EncodeResult): EncodeResult =
      EncodeInsufficientBuffer(new ContinuationEncoder {
        override def encode(buffer: MutableDirectBuffer, writeOffset: Int): EncodeResult =
          continuationEncoder.encode(buffer, writeOffset).andThen(r)
      })
  }

}
