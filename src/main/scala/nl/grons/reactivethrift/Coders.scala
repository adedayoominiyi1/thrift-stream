package nl.grons.reactivethrift

import java.nio.charset.StandardCharsets

import nl.grons.reactivethrift.decoders.Decoder

// Protocol

trait Protocol {
  def structDecoder[A](structBuilder: Factory[StructBuilder]): Decoder[A]
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

// Coders

// trait Coder[A] extends Encoder[A] with Decoder[A]

// Builders

trait StructBuilder {
  def build(): AnyRef

  def collectionBuilderForField(fieldId: Short): Int => CollectionBuilder

  def mapBuilderForField(fieldId: Short): Int => MapBuilder

  def structBuilderForField(fieldId: Short): Factory[StructBuilder]

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

object StructBuilder {
  val IgnoreAllFactory: Factory[StructBuilder] = {
    val ignoreAllStructBuilder = new IgnoreAllStructBuilder {}
    () => ignoreAllStructBuilder
  }

  // TODO: move somewere else, to the protocol?
  def toString(binary: Array[Byte]): String = new String(binary, StandardCharsets.UTF_8)
  def toBinary(string: String): Array[Byte] = string.getBytes(StandardCharsets.UTF_8)
}

class IgnoreAllStructBuilder extends StructBuilder {
  override def build(): AnyRef = null

  override def collectionBuilderForField(fieldId: Short) = CollectionBuilder.IgnoreAllFactory

  override def mapBuilderForField(fieldId: Short) = MapBuilder.IgnoreAllFactory

  override def structBuilderForField(fieldId: Short) = StructBuilder.IgnoreAllFactory

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

  def collectionBuilderForItem(): Int => CollectionBuilder = CollectionBuilder.IgnoreAllFactory

  def mapBuilderForItem(): Int => MapBuilder = MapBuilder.IgnoreAllFactory

  def structBuilderForItem(): Factory[StructBuilder] = StructBuilder.IgnoreAllFactory

  def readItem(value: Any): Unit
}

object CollectionBuilder {
  val IgnoreAllFactory: Int => CollectionBuilder = {
    val ignoreAllCollectionBuilder = new CollectionBuilder {
      override def readItem(value: Any): Unit = {}
      override def build(): AnyRef = null
    }
    _ => ignoreAllCollectionBuilder
  }
}

trait MapBuilder {
  def build(): AnyRef

  def collectionBuilderForKey(): Int => CollectionBuilder = CollectionBuilder.IgnoreAllFactory

  def mapBuilderForKey(): Int => MapBuilder = MapBuilder.IgnoreAllFactory

  def structBuilderForKey(): Factory[StructBuilder] = StructBuilder.IgnoreAllFactory

  def collectionBuilderForValue(): Int => CollectionBuilder = CollectionBuilder.IgnoreAllFactory

  def mapBuilderForValue(): Int => MapBuilder = MapBuilder.IgnoreAllFactory

  def structBuilderForValue(): Factory[StructBuilder] = StructBuilder.IgnoreAllFactory

  def readItem(key: Any, value: Any): Unit
}

object MapBuilder {
  val IgnoreAllFactory: Int => MapBuilder = {
    val ignoreAllMapBuilder = new MapBuilder {
      override def build(): AnyRef = null
      override def readItem(key: Any, value: Any): Unit = {}
    }
    _ => ignoreAllMapBuilder
  }
}
