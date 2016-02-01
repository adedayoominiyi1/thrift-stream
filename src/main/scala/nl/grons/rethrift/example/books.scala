package nl.grons.rethrift.example

import java.nio.charset.StandardCharsets

import nl.grons.rethrift._

// struct Author {
//   1: string name
// }
//
// struct Book {
//   1: string title,
//   2: Author author,
//   4: int32 year,
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

  def startStruct(fieldId: Short): StructBuilder[_] = AnonymousStructBuilder
  def stopStruct(): IAuthor = builder
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
  // TODO: what about list, set, map ???
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}

// --------------- Book ---------------
trait IBook {
  def title: String
  def author: IAuthor
  def year: Int
  def build(): Book
}

case class Book(title: String, author: Author, year: Int) extends IBook {
  override def build(): Book = this
}

class BookBuilder(private var _title: String, private var _author: IAuthor, private var _year: Int) extends IBook {
  def this() {
    this(null, null, 0)
  }

  def this(book: IBook) {
    this(book.title, book.author, book.year)
  }

  def title = _title
  def author = _author
  def year = _year

  def withTitle(title: String): BookBuilder = { this._title = title; this; }
  def withAuthor(author: IAuthor): BookBuilder = { this._author = author; this; }
  def withYear(year: Int): BookBuilder = { this._year = year; this; }

  def build(): Book = Book(_title, _author.build(), _year)
}

class BookStructBuilder extends StructBuilder[IBook] {
  private[this] val builder = new BookBuilder()

  def startStruct(fieldId: Short): StructBuilder[_] = {
    fieldId match {
      case 2 => new AuthorStructBuilder
      case _ => AnonymousStructBuilder
    }
  }
  def stopStruct(): IBook = builder
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
  // TODO: what about list, set, map ???
  def readStruct(fieldId: Short, fieldValue: Any): Unit = {}
}
