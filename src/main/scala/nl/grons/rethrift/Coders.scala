package nl.grons.rethrift

import uk.co.real_logic.agrona.DirectBuffer

trait Protocol {
  def structDecoder[A](structBuilder: StructBuilder[A]): Decoder[A]
}

// Encoders, Decoders, Coders

sealed abstract class DecodeResult[A] {
  def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B]
}

case class Decoded[A](value: A, notConsumed: DirectBuffer, notConsumedBufferReadOffset: Int) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
    function(value, notConsumed, notConsumedBufferReadOffset)
  }
}

case class DecodeFailure[A](error: String) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = DecodeFailure(error)
}

case class DecodeUnsufficientData[A](e: Decoder[A]) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
    // Return a decoder that can continue later, first handling the given function
    DecodeUnsufficientData(new Decoder[B] {
      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[B] = {
        e.decode(buffer, readOffset).andThen(function)
      }
    })
  }
}

sealed abstract class EncodeResult[A]
case class EncodeFailure[A](e: Exception) extends EncodeResult[A]
case class Encoded[A](value: A, notConsumed: DirectBuffer) extends EncodeResult[A]

trait Encoder[A] {
  def encode(a: A, bufferFactory: () => DirectBuffer): EncodeResult[A]
}

trait Decoder[A] {
  def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A]
}

trait Coder[A] extends Encoder[A] with Decoder[A]

trait StructBuilder[A] {
  def startStruct(fieldId: Short): StructBuilder[_]

  def stopStruct(): A

  def readBoolean(fieldId: Short, value: Boolean): Unit

  def readInt8(fieldId: Short, value: Byte): Unit

  def readInt16(fieldId: Short, value: Short): Unit

  def readInt32(fieldId: Short, value: Int): Unit

  def readInt64(fieldId: Short, value: Long): Unit

  def readDouble(fieldId: Short, value: Double): Unit

  def readBinary(fieldId: Short, value: Array[Byte]): Unit

  // TODO: what about list, set, map ???

  def readStruct(fieldId: Short, fieldValue: Any): Unit
}

object AnonymousStructBuilder extends StructBuilder[Unit] {
  def startStruct(fieldId: Short): StructBuilder[_] = AnonymousStructBuilder

  def stopStruct(): Unit = {}

  def readBoolean(fieldId: Short, value: Boolean): Unit = {}

  def readInt8(fieldId: Short, value: Byte): Unit = {}

  def readInt16(fieldId: Short, value: Short): Unit = {}

  def readInt32(fieldId: Short, value: Int): Unit = {}

  def readInt64(fieldId: Short, value: Long): Unit = {}

  def readDouble(fieldId: Short, value: Double): Unit = {}

  def readBinary(fieldId: Short, value: Array[Byte]): Unit = {}

  // TODO: what about list, set, map ???

  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

