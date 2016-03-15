package nl.grons.reactivethrift.example

import java.nio.charset.StandardCharsets

import nl.grons.reactivethrift._

import scala.collection.mutable.ArrayBuffer

// This file contains the code that would be generated from a thrift IDL file.

// struct Author {
//   1: string name
// }
//
// struct Book {
//   1: string title,
//   2: Author author,
//   4: int32 year,
//   5: list<int8> pages,
//   6: map<string, Book> alternatives,
// }

case class Author(
  name: String
)

case class Book(
  title: String,
  author: Author,
  year: Int,
  pages: Seq[Byte],
  alternatives: Map[String, Book]
)

// --------------- Author ---------------

class AuthorStructBuilder extends StructBuilder {
  private[this] var name: String = _

  override def collectionBuilderForField(fieldId: Short): Int => CollectionBuilder = _ => BlackHoleCollectionBuilder
  override def mapBuilderForField(fieldId: Short): Int => MapBuilder = _ => BlackHoleMapBuilder
  override def structBuilderForField(fieldId: Short): () => StructBuilder = () => BlackHoleStructBuilder
  override def build(): Author = {
    // TODO: Check presence of required fields
    Author(name)
  }
  override def readBoolean(fieldId: Short, fieldValue: Boolean): Unit = {}
  override def readInt8(fieldId: Short, fieldValue: Byte): Unit = {}
  override def readInt16(fieldId: Short, fieldValue: Short): Unit = {}
  override def readInt32(fieldId: Short, fieldValue: Int): Unit = {}
  override def readInt64(fieldId: Short, fieldValue: Long): Unit = {}
  override def readDouble(fieldId: Short, fieldValue: Double): Unit = {}
  override def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
    fieldId match {
      case 1 => this.name = new String(fieldValue, StandardCharsets.UTF_8)
      case _ => ()
    }
  }
  override def readCollection(fieldId: Short, fieldValue: Any): Unit = {}
  override def readMap(fieldId: Short, fieldValue: Any): Unit = {}
  override def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

// --------------- Book ---------------

class BookStructBuilder extends StructBuilder {
  private[this] var title: String = _
  private[this] var author: Author = _
  private[this] var year: Int = _
  private[this] var pages: Seq[Byte] = Seq.empty
  private[this] var alternatives: Map[String, Book] = Map.empty

  override def build() =  {
    // TODO: Check presence of required fields
    Book(title, author, year, pages, alternatives)
  }
  override def collectionBuilderForField(fieldId: Short): Int => CollectionBuilder = {
    fieldId match {
      case 5 => size: Int => new ByteSeqBuilder(size)
      case _ => _ => BlackHoleCollectionBuilder
    }
  }
  override def mapBuilderForField(fieldId: Short): Int => MapBuilder = {
    fieldId match {
      case 5 => (size: Int) => new MapBuilder() {
        private[this] val items = Map.newBuilder[String, Book]

        override def structBuilderForValue(): () => StructBuilder = () => new BookStructBuilder()

        override def readItem(key: Any, value: Any): Unit = {
          items.+=((new String(key.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8), value.asInstanceOf[Book]))
        }

        override def build() = items.result()
      }
      case _ => _ => BlackHoleMapBuilder
    }
  }
  override def structBuilderForField(fieldId: Short): () => StructBuilder = {
    fieldId match {
      case 2 => () => new AuthorStructBuilder()
      case _ => () => BlackHoleStructBuilder
    }
  }
  override def readBoolean(fieldId: Short, fieldValue: Boolean): Unit = {}
  override def readInt8(fieldId: Short, fieldValue: Byte): Unit = {}
  override def readInt16(fieldId: Short, fieldValue: Short): Unit = {}
  override def readInt32(fieldId: Short, fieldValue: Int): Unit = {
    fieldId match {
      case 4 => this.year = fieldValue
      case _ => ()
    }
  }
  override def readInt64(fieldId: Short, fieldValue: Long): Unit = {}
  override def readDouble(fieldId: Short, fieldValue: Double): Unit = {}
  override def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
    fieldId match {
      case 1 => this.title = new String(fieldValue, StandardCharsets.UTF_8)
      case _ => ()
    }
  }
  override def readCollection(fieldId: Short, fieldValue: Any): Unit = {
    fieldId match {
      case 5 => this.pages = fieldValue.asInstanceOf[Seq[Byte]]
      case _ => ()
    }
  }
  override def readMap(fieldId: Short, fieldValue: Any): Unit = {
    fieldId match {
      case 6 => this.alternatives = fieldValue.asInstanceOf[Map[String, Book]]
      case _ => ()
    }
  }
  override def readStruct(fieldId: Short, fieldValue: Any): Unit = {
    fieldId match {
      case 2 => this.author = fieldValue.asInstanceOf[Author]
      case _ => ()
    }
  }
}

class ByteSeqBuilder(size: Int) extends CollectionBuilder {
  private[this] val items = new ArrayBuffer[Byte](size)

  override def build(): Seq[Byte] = items.toSeq

  override def readItem(value: Any): Unit = {
    items += value.asInstanceOf[Byte]
  }
}
