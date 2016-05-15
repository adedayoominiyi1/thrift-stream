package nl.grons.reactivethrift.encoder

import uk.co.real_logic.agrona.MutableDirectBuffer

sealed abstract class EncodeResult

object EncodeResult {

  case class Encoded(buffer: MutableDirectBuffer, nextWriteOffset: Int) extends EncodeResult

  case class EncodeFailure(error: String) extends EncodeResult

  case class EncodeInsufficientBuffer(continuationEncoder: ContinuationEncoder) extends EncodeResult

}
