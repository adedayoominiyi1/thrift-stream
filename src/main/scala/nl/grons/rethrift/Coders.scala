package nl.grons.rethrift

import uk.co.real_logic.agrona.DirectBuffer

import scala.annotation.tailrec

// Protocol

trait Protocol {
  def structDecoder[A](structBuilder: StructBuilder[A]): Decoder[A]
}

// Decoders

trait Decoder[A] {
  def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A]
}

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

case class DecodeUnsufficientData[A](continuationDecoder: Decoder[A]) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
    // Return a decoder that can continue later, first handling the given function
    DecodeUnsufficientData(new Decoder[B] {
      override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[B] = {
        continuationDecoder.decode(buffer, readOffset).andThen(function)
      }
    })
  }
}

/** Trampoline continuation. See [[Decoder.trampoliningDecoder]]. */
case class Continue[A](private val thunk: () => DecodeResult[A]) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
    thunk().andThen(function)
  }
}

object Decoder {
  /**
    * A [[Decoder]] that only returns [[Decoded]], [[DecodeFailure]] or [[DecodeUnsufficientData]]
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
    new Decoder[A] {
      def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A] = {
        trampoline(decoder.decode(buffer, readOffset))
      }
    }
  }

  @tailrec
  private def trampoline[A](decodeResult: DecodeResult[A]): DecodeResult[A] = {
    decodeResult match {
      case Continue(thunk) => trampoline(thunk())
      case result => result
    }
  }
}

// Encoders

sealed abstract class EncodeResult[A]
case class EncodeFailure[A](e: Exception) extends EncodeResult[A]
case class Encoded[A](value: A, notConsumed: DirectBuffer) extends EncodeResult[A]

trait Encoder[A] {
  def encode(a: A, bufferFactory: () => DirectBuffer): EncodeResult[A]
}

// Coders

trait Coder[A] extends Encoder[A] with Decoder[A]

trait StructBuilder[A] {
  def build(): A

  def listBuilderForField(fieldId: Short): ListBuilder[_]

  def structBuilderForField(fieldId: Short): StructBuilder[_]

  def readBoolean(fieldId: Short, value: Boolean): Unit

  def readInt8(fieldId: Short, value: Byte): Unit

  def readInt16(fieldId: Short, value: Short): Unit

  def readInt32(fieldId: Short, value: Int): Unit

  def readInt64(fieldId: Short, value: Long): Unit

  def readDouble(fieldId: Short, value: Double): Unit

  def readBinary(fieldId: Short, value: Array[Byte]): Unit

  def readList(fieldId: Short, value: Seq[_]): Unit

  // TODO: add set, map

  def readStruct(fieldId: Short, value: Any): Unit
}

object BlackHoleStructBuilder extends StructBuilder[Unit] {
  def build(): Unit = {}

  def listBuilderForField(fieldId: Short): ListBuilder[_] = BlackHoleListBuilder

  def structBuilderForField(fieldId: Short): StructBuilder[_] = BlackHoleStructBuilder

  def readBoolean(fieldId: Short, value: Boolean): Unit = {}

  def readInt8(fieldId: Short, value: Byte): Unit = {}

  def readInt16(fieldId: Short, value: Short): Unit = {}

  def readInt32(fieldId: Short, value: Int): Unit = {}

  def readInt64(fieldId: Short, value: Long): Unit = {}

  def readDouble(fieldId: Short, value: Double): Unit = {}

  def readBinary(fieldId: Short, value: Array[Byte]): Unit = {}

  def readList(fieldId: Short, value: Seq[_]): Unit = {}

  def readStruct(fieldId: Short, value: Any): Unit = {}
}

trait ListBuilder[A] {
  def init(size: Int): Unit

  def build(): Seq[A]

  def listBuilderForItem(): ListBuilder[_] = BlackHoleListBuilder

  def structBuilderForItem(): StructBuilder[_] = BlackHoleStructBuilder

  def readItem(value: A): Unit

  // TODO: add set, map
}

object BlackHoleListBuilder extends ListBuilder[Unit] {
  def init(size: Int): Unit = {}

  def build(): Seq[Unit] = Seq.empty

  def readItem(value: Unit): Unit = {}
}
