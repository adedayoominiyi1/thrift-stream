package nl.grons.reactivethrift.encoder

import uk.co.real_logic.agrona.MutableDirectBuffer

sealed abstract class EncodeResult[A]

object EncodeResult {

  case class Encoded[A](buffer: MutableDirectBuffer, nextWriteOffset: Int) extends EncodeResult[A]

  case class EncodeFailure[A](error: String) extends EncodeResult[A]

  case class EncodeInsufficientBuffer[A](continuationEncoder: Encoder[A]) extends EncodeResult[A]

}