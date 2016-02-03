package nl.grons.rethrift.example

import java.nio.charset.StandardCharsets

import nl.grons.rethrift._

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

  def listBuilderForField(fieldId: Short): ListBuilder[_] = BlackHoleListBuilder
  def structBuilderForField(fieldId: Short): StructBuilder[_] = BlackHoleStructBuilder
  def build(): IAuthor = builder
  def readBoolean(fieldId: Short, value: Boolean): Unit = {}
  def readInt8(fieldId: Short, value: Byte): Unit = {}
  def readInt16(fieldId: Short, value: Short): Unit = {}
  def readInt32(fieldId: Short, value: Int): Unit = {}
  def readInt64(fieldId: Short, value: Long): Unit = {}
  def readDouble(fieldId: Short, value: Double): Unit = {}
  def readBinary(fieldId: Short, value: Array[Byte]): Unit = {
    fieldId match {
      case 1 => builder.withName(new String(value, StandardCharsets.UTF_8))
      case _ => ()
    }
  }
  def readList(fieldId: Short, value: Seq[_]): Unit = {}
  // TODO: add set and map
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

// --------------- Book ---------------
trait IBook {
  def title: String
  def author: IAuthor
  def year: Int
  def pages: Seq[Byte]
  def build(): Book
}

case class Book(title: String, author: Author, year: Int, pages: Seq[Byte]) extends IBook {
  override def build(): Book = this
}

class BookBuilder(private var _title: String, private var _author: IAuthor, private var _year: Int, private var _pages: Seq[Byte]) extends IBook {
  def this() {
    this(null, null, 0, Seq.empty)
  }

  def this(book: IBook) {
    this(book.title, book.author, book.year, book.pages)
  }

  def title = _title
  def author = _author
  def year = _year
  def pages = _pages

  def withTitle(title: String): BookBuilder = { this._title = title; this; }
  def withAuthor(author: IAuthor): BookBuilder = { this._author = author; this; }
  def withYear(year: Int): BookBuilder = { this._year = year; this; }
  def withPages(pages: Seq[Byte]): BookBuilder = { this._pages = pages; this; }

  def build(): Book = Book(_title, _author.build(), _year, _pages)
}

class BookStructBuilder extends StructBuilder[IBook] {
  private[this] val builder = new BookBuilder()

  def build(): IBook = builder
  def listBuilderForField(fieldId: Short): ListBuilder[_] = {
    fieldId match {
      case 5 => new PagesListBuilder()
      case _ => BlackHoleListBuilder
    }
  }
  def structBuilderForField(fieldId: Short): StructBuilder[_] = {
    fieldId match {
      case 2 => new AuthorStructBuilder
      case _ => BlackHoleStructBuilder
    }
  }
  def readBoolean(fieldId: Short, value: Boolean): Unit = {}
  def readInt8(fieldId: Short, value: Byte): Unit = {}
  def readInt16(fieldId: Short, value: Short): Unit = {}
  def readInt32(fieldId: Short, value: Int): Unit = {
    fieldId match {
      case 4 => builder.withYear(value)
      case _ => ()
    }
  }
  def readInt64(fieldId: Short, value: Long): Unit = {}
  def readDouble(fieldId: Short, value: Double): Unit = {}
  def readBinary(fieldId: Short, value: Array[Byte]): Unit = {
    fieldId match {
      case 1 => builder.withTitle(new String(value, StandardCharsets.UTF_8))
      case _ => ()
    }
  }
  def readList(fieldId: Short, value: Seq[_]): Unit = {
    fieldId match {
      case 5 => builder.withPages(value.asInstanceOf[Seq[Byte]])
      case _ => ()
    }
  }
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

class PagesListBuilder extends ListBuilder[Byte] {
  // TODO: for primitive values, use a builder that doesn't use primitive wrappers
  private var items: ArrayBuffer[Byte] = _

  def init(size: Int): Unit = {
    items = new ArrayBuffer[Byte](size)
  }

  def build(): Seq[Byte] = items.toSeq

  def readItem(value: Byte): Unit = {
    items += value
  }

  def readListBegin(fieldId: Short): ListBuilder[_] = BlackHoleListBuilder
}
