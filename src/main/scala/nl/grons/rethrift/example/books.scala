package nl.grons.rethrift.example

import java.nio.charset.StandardCharsets

import nl.grons.rethrift._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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

// --------------- Author ---------------

trait IAuthor {
  def name: String
  def build(): Author
}

case class Author(name: String) extends IAuthor {
  override def build(): Author = this
}

class AuthorBuilder(private var _name: String) extends IAuthor {
  def this() {
    this(null: String)
  }

  def this(author: IAuthor) {
    this(author.name)
  }

  def name = _name

  def withName(name: String): AuthorBuilder = { this._name = name; this; }

  def build(): Author = Author(_name)
}

class AuthorStructBuilder extends StructBuilder[IAuthor] {
  private[this] val builder = new AuthorBuilder()

  def collectionBuilderForField(fieldId: Short): CollectionBuilder[_] = BlackHoleCollectionBuilder
  def mapBuilderForField(fieldId: Short): MapBuilder[_] = BlackHoleMapBuilder
  def structBuilderForField(fieldId: Short): StructBuilder[_] = BlackHoleStructBuilder
  def build(): IAuthor = {
    // Check presence of required fields
    builder
  }
  def readBoolean(fieldId: Short, fieldValue: Boolean): Unit = {}
  def readInt8(fieldId: Short, fieldValue: Byte): Unit = {}
  def readInt16(fieldId: Short, fieldValue: Short): Unit = {}
  def readInt32(fieldId: Short, fieldValue: Int): Unit = {}
  def readInt64(fieldId: Short, fieldValue: Long): Unit = {}
  def readDouble(fieldId: Short, fieldValue: Double): Unit = {}
  def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
    fieldId match {
      case 1 => builder.withName(new String(fieldValue, StandardCharsets.UTF_8))
      case _ => ()
    }
  }
  def readCollection(fieldId: Short, fieldValue: Any): Unit = {}
  def readMap(fieldId: Short, fieldValue: Any): Unit = {}
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

// --------------- Book ---------------
trait IBook {
  def title: String
  def author: IAuthor
  def year: Int
  def pages: Seq[Byte]
  def alternatives: Map[String, IBook]
  def build(): Book
}

case class Book(title: String, author: Author, year: Int, pages: Seq[Byte], alternatives: Map[String, IBook]) extends IBook {
  override def build(): Book = this
}

class BookBuilder(private var _title: String, private var _author: IAuthor, private var _year: Int, private var _pages: Seq[Byte], private var _alternatives: Map[String, IBook]) extends IBook {
  def this() {
    this(null, null, 0, Seq.empty, Map.empty)
  }

  def this(book: IBook) {
    this(book.title, book.author, book.year, book.pages, book.alternatives)
  }

  def title = _title
  def author = _author
  def year = _year
  def pages = _pages
  def alternatives = _alternatives

  def withTitle(title: String): BookBuilder = { this._title = title; this; }
  def withAuthor(author: IAuthor): BookBuilder = { this._author = author; this; }
  def withYear(year: Int): BookBuilder = { this._year = year; this; }
  def withPages(pages: Seq[Byte]): BookBuilder = { this._pages = pages; this; }
  def withAlternatives(alternatives: Map[String, IBook]): BookBuilder = { this._alternatives = alternatives; this; }

  def build(): Book = Book(_title, _author.build(), _year, _pages, _alternatives)
}

class BookStructBuilder extends StructBuilder[IBook] {
  private[this] val builder = new BookBuilder()

  def build(): IBook =  {
    // Check presence of required fields
    builder
  }
  def collectionBuilderForField(fieldId: Short): CollectionBuilder[_] = {
    fieldId match {
      case 5 => new ByteSeqBuilder()
      case _ => BlackHoleCollectionBuilder
    }
  }
  def mapBuilderForField(fieldId: Short): MapBuilder[_] = {
    fieldId match {
      case 5 => new MapBuilder[Map[String, IBook]]() {
        private var items: mutable.Builder[(String, IBook), Map[String, IBook]] = _

        override def structBuilderForValue(): StructBuilder[_] = new BookStructBuilder()

        override def init(size: Int): Unit = {
          items = Map.newBuilder[String, IBook]
        }

        override def readItem(key: Any, value: Any): Unit = {
          items.+=((new String(key.asInstanceOf[Array[Byte]], StandardCharsets.UTF_8), value.asInstanceOf[IBook]))
        }

        override def build(): Map[String, IBook] = items.result()
      }
      case _ => BlackHoleMapBuilder
    }
  }
  def structBuilderForField(fieldId: Short): StructBuilder[_] = {
    fieldId match {
      case 2 => new AuthorStructBuilder()
      case _ => BlackHoleStructBuilder
    }
  }
  def readBoolean(fieldId: Short, fieldValue: Boolean): Unit = {}
  def readInt8(fieldId: Short, fieldValue: Byte): Unit = {}
  def readInt16(fieldId: Short, fieldValue: Short): Unit = {}
  def readInt32(fieldId: Short, fieldValue: Int): Unit = {
    fieldId match {
      case 4 => builder.withYear(fieldValue)
      case _ => ()
    }
  }
  def readInt64(fieldId: Short, fieldValue: Long): Unit = {}
  def readDouble(fieldId: Short, fieldValue: Double): Unit = {}
  def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
    fieldId match {
      case 1 => builder.withTitle(new String(fieldValue, StandardCharsets.UTF_8))
      case _ => ()
    }
  }
  def readCollection(fieldId: Short, fieldValue: Any): Unit = {
    fieldId match {
      case 5 => builder.withPages(fieldValue.asInstanceOf[Seq[Byte]])
      case _ => ()
    }
  }
  def readMap(fieldId: Short, fieldValue: Any): Unit = {}
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {
    fieldId match {
      case 2 => builder.withAuthor(fieldValue.asInstanceOf[IAuthor])
      case _ => ()
    }
  }
}

class ByteSeqBuilder extends CollectionBuilder[Seq[Byte]] {
  // TODO: for primitive values, use a builder that doesn't use primitive wrappers
  private var items: ArrayBuffer[Byte] = _

  def init(size: Int): Unit = {
    items = new ArrayBuffer[Byte](size)
  }

  def build(): Seq[Byte] = items.toSeq

  def readItem(value: Any): Unit = {
    items += value.asInstanceOf[Byte]
  }
}
