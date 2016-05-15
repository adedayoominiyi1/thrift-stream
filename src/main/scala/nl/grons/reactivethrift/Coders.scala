package nl.grons.reactivethrift

import nl.grons.reactivethrift.decoders.Decoder
import nl.grons.reactivethrift.encoder.Encoder
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
