package nl.grons.reactivethrift

import uk.co.real_logic.agrona.DirectBuffer

import scala.annotation.tailrec

// Protocol

trait Protocol {
  def structDecoder[A](structBuilder: () => StructBuilder): Decoder[A]
}

// Decoders

trait Decoder[A] {
  def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[A]
}

// TODO: rewrite DecodeResult to following pattern
// sealed trait Result
// object Result{
//   case class Success(value: String) extends Result
//   case class Failure(msg: String) extends Result
//   case class Error(msg: String) extends Result
// }

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

// Builders

trait StructBuilder {
  def build(): AnyRef

  def collectionBuilderForField(fieldId: Short): Int => CollectionBuilder

  def mapBuilderForField(fieldId: Short): Int => MapBuilder

  def structBuilderForField(fieldId: Short): () => StructBuilder

  def readBoolean(fieldId: Short, value: Boolean): Unit

  def readInt8(fieldId: Short, value: Byte): Unit

  def readInt16(fieldId: Short, value: Short): Unit

  def readInt32(fieldId: Short, value: Int): Unit

  def readInt64(fieldId: Short, value: Long): Unit

  def readDouble(fieldId: Short, value: Double): Unit

  def readBinary(fieldId: Short, value: Array[Byte]): Unit

  def readCollection(fieldId: Short, value: Any): Unit

  def readMap(fieldId: Short, value: Any): Unit

  def readStruct(fieldId: Short, value: Any): Unit
}

object BlackHoleStructBuilder extends StructBuilder {
  override def build(): AnyRef = null

  override def collectionBuilderForField(fieldId: Short) = _ => BlackHoleCollectionBuilder

  override def mapBuilderForField(fieldId: Short) = _ => BlackHoleMapBuilder

  override def structBuilderForField(fieldId: Short) = () => BlackHoleStructBuilder

  override def readBoolean(fieldId: Short, value: Boolean): Unit = {}

  override def readInt8(fieldId: Short, value: Byte): Unit = {}

  override def readInt16(fieldId: Short, value: Short): Unit = {}

  override def readInt32(fieldId: Short, value: Int): Unit = {}

  override def readInt64(fieldId: Short, value: Long): Unit = {}

  override def readDouble(fieldId: Short, value: Double): Unit = {}

  override def readBinary(fieldId: Short, value: Array[Byte]): Unit = {}

  override def readCollection(fieldId: Short, value: Any): Unit = {}

  override def readMap(fieldId: Short, value: Any): Unit = {}

  override def readStruct(fieldId: Short, value: Any): Unit = {}
}

trait CollectionBuilder {
  def build(): AnyRef

  def collectionBuilderForItem(): Int => CollectionBuilder = _ => BlackHoleCollectionBuilder

  def mapBuilderForItem(): Int => MapBuilder = _ => BlackHoleMapBuilder

  def structBuilderForItem(): () => StructBuilder = () => BlackHoleStructBuilder

  def readItem(value: Any): Unit
}

object BlackHoleCollectionBuilder extends CollectionBuilder {
  override def build(): AnyRef = null

  override def readItem(value: Any): Unit = {}
}

trait MapBuilder {
  def build(): AnyRef

  def collectionBuilderForKey(): Int => CollectionBuilder = _ => BlackHoleCollectionBuilder

  def mapBuilderForKey(): Int => MapBuilder = _ => BlackHoleMapBuilder

  def structBuilderForKey(): () => StructBuilder = () => BlackHoleStructBuilder

  def collectionBuilderForValue(): Int => CollectionBuilder = _ => BlackHoleCollectionBuilder

  def mapBuilderForValue(): Int => MapBuilder = _ => BlackHoleMapBuilder

  def structBuilderForValue(): () => StructBuilder = () => BlackHoleStructBuilder

  def readItem(key: Any, value: Any): Unit
}

object BlackHoleMapBuilder extends MapBuilder {
  override def build(): AnyRef = null

  override def readItem(key: Any, value: Any): Unit = {}
}
