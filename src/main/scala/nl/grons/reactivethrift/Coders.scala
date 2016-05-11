package nl.grons.reactivethrift

import uk.co.real_logic.agrona.DirectBuffer

import scala.annotation.tailrec

// Protocol

trait Protocol {
  def structDecoder[A](structBuilder: () => StructBuilder): Decoder[A]
}

object Protocol {
  case class TMessage(name: String, messageType: Byte, sequenceId: Int)
}

object MessageType {
  val STOP: Byte = 0
  val VOID: Byte = 1
  val BOOL: Byte = 2
  val BYTE: Byte = 3
  val DOUBLE: Byte = 4
  val I16: Byte = 6
  val I32: Byte = 8
  val I64: Byte = 10
  val STRING: Byte = 11
  val STRUCT: Byte = 12
  val MAP: Byte = 13
  val SET: Byte = 14
  val LIST: Byte = 15
  val ENUM: Byte = 16
}

// Decoders

object Decoder {

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

  def zip[A, B](aDecoder: Decoder[A], bDecoder: Decoder[B]): Decoder[(A, B)] = new Decoder[(A, B)] {
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

  def zip3[A, B, C](aDecoder: Decoder[A], bDecoder: Decoder[B], cDecoder: Decoder[C]): Decoder[(A, B, C)] = new Decoder[(A, B, C)] {
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

  def zip4[A, B, C, D](aDecoder: Decoder[A], bDecoder: Decoder[B], cDecoder: Decoder[C], dDecoder: Decoder[D]): Decoder[(A, B, C, D)] = new Decoder[(A, B, C, D)] {
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

  def trampolined: Decoder[A] = Decoder.trampoliningDecoder(self)

  def zip[B](bDecoder: Decoder[B]): Decoder[(A, B)] = Decoder.zip(self, bDecoder)
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

case class DecodeInsufficientData[A](continuationDecoder: Decoder[A]) extends DecodeResult[A] {
  override def andThen[B](function: (A, DirectBuffer, Int) => DecodeResult[B]): DecodeResult[B] = {
    // Return a decoder that can continue later, first handling the given function
    DecodeInsufficientData(new Decoder[B] {
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

  def readBoolean(fieldId: Short, fieldValue: Boolean): Unit

  def readInt8(fieldId: Short, fieldValue: Byte): Unit

  def readInt16(fieldId: Short, fieldValue: Short): Unit

  def readInt32(fieldId: Short, fieldValue: Int): Unit

  def readInt64(fieldId: Short, fieldValue: Long): Unit

  def readDouble(fieldId: Short, fieldValue: Double): Unit

  def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit

  def readCollection(fieldId: Short, fieldValue: Any): Unit

  def readMap(fieldId: Short, fieldValue: Any): Unit

  def readStruct(fieldId: Short, fieldValue: Any): Unit
}

class IgnoreAllStructBuilder extends StructBuilder {
  override def build(): AnyRef = null

  override def collectionBuilderForField(fieldId: Short) = _ => IgnoreAllCollectionBuilder

  override def mapBuilderForField(fieldId: Short) = _ => IgnoreAllMapBuilder

  override def structBuilderForField(fieldId: Short) = () => IgnoreAllStructBuilder

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

object IgnoreAllStructBuilder extends IgnoreAllStructBuilder

trait CollectionBuilder {
  def build(): AnyRef

  def collectionBuilderForItem(): Int => CollectionBuilder = _ => IgnoreAllCollectionBuilder

  def mapBuilderForItem(): Int => MapBuilder = _ => IgnoreAllMapBuilder

  def structBuilderForItem(): () => StructBuilder = () => IgnoreAllStructBuilder

  def readItem(value: Any): Unit
}

object IgnoreAllCollectionBuilder extends CollectionBuilder {
  override def build(): AnyRef = null

  override def readItem(value: Any): Unit = {}
}

trait MapBuilder {
  def build(): AnyRef

  def collectionBuilderForKey(): Int => CollectionBuilder = _ => IgnoreAllCollectionBuilder

  def mapBuilderForKey(): Int => MapBuilder = _ => IgnoreAllMapBuilder

  def structBuilderForKey(): () => StructBuilder = () => IgnoreAllStructBuilder

  def collectionBuilderForValue(): Int => CollectionBuilder = _ => IgnoreAllCollectionBuilder

  def mapBuilderForValue(): Int => MapBuilder = _ => IgnoreAllMapBuilder

  def structBuilderForValue(): () => StructBuilder = () => IgnoreAllStructBuilder

  def readItem(key: Any, value: Any): Unit
}

object IgnoreAllMapBuilder extends MapBuilder {
  override def build(): AnyRef = null

  override def readItem(key: Any, value: Any): Unit = {}
}
