package nl.grons.thriftstream.example

import nl.grons.thriftstream._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

// This file contains the code that would be generated from a thrift IDL file.

// #@namespace scala com.example.books.thriftscala
// namespace java com.example.books.thriftjava
//
// struct Author {
//   1: string name
// }
//
// struct Book {
//   1: string title
//   2: Author author
//   4: i32 year
//   5: list<i8> pages
//   6: map<string, Book> alternatives
// }
//
// service BookService {
//   Book bookByTitle(1: string title)
//   list<Book> booksForAuthor(1: string authorName)
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

class AuthorStructBuilder extends IgnoreAllStructBuilder {
  private[this] var name: String = _

  override def build(): Author = {
    // TODO: Check presence of required fields
    Author(name)
  }

  override def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
    fieldId match {
      case 1 => this.name = StructBuilder.toString(fieldValue)
      case _ => ()
    }
  }
}

trait StructWriter {
  def fieldsToEncode: Seq[StructField]
  def writeI8(fieldId: Short): Byte
  def writeI16(fieldId: Short): Short
  def writeI32(fieldId: Short): Int
  def writeI64(fieldId: Short): Long
  def writeDouble(fieldId: Short): Double
  def writeBoolean(fieldId: Short): Boolean
  def writeBinary(fieldId: Short): Array[Byte]
  def writeStruct(fieldId: Short): StructWriter
  def writeCollection(fieldId: Short): (ThriftType, Iterator[_])
  def writeMap(fieldId: Short): (ThriftType, ThriftType, Iterator[(_, _)])
}

class AuthorStructWriter(author: Author) extends StructWriter {
  private val nameField = StructField(1, ThriftType.Binary)

  override val fieldsToEncode: Seq[StructField] = {
    val fields = new mutable.ArrayBuffer[StructField](1)
    if (author.name != null) fields += nameField
    fields
  }

  def writeI8(fieldId: Short): Byte = ???
  def writeI16(fieldId: Short): Short = ???
  def writeI32(fieldId: Short): Int = ???
  def writeI64(fieldId: Short): Long = ???
  def writeDouble(fieldId: Short): Double = ???
  def writeBoolean(fieldId: Short): Boolean = ???
  def writeBinary(fieldId: Short): Array[Byte] = fieldId match {
    case 1 => StructBuilder.toBinary(author.name)
  }
  def writeStruct(fieldId: Short): StructWriter = ???
  def writeCollection(fieldId: Short): (ThriftType, Iterator[_]) = ???
  def writeMap(fieldId: Short): (ThriftType, ThriftType, Iterator[(_, _)]) = ???
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
      case _ => CollectionBuilder.IgnoreAllFactory
    }
  }
  override def mapBuilderForField(fieldId: Short): Int => MapBuilder = {
    fieldId match {
      case 5 => (size: Int) => new MapBuilder() {
        private[this] val items = Map.newBuilder[String, Book]

        override def structBuilderForValue(): () => StructBuilder = () => new BookStructBuilder()

        override def readItem(key: Any, value: Any): Unit = {
          items.+=((StructBuilder.toString(key.asInstanceOf[Array[Byte]]), value.asInstanceOf[Book]))
        }

        override def build() = items.result()
      }
      case _ => MapBuilder.IgnoreAllFactory
    }
  }
  override def structBuilderForField(fieldId: Short): () => StructBuilder = {
    fieldId match {
      case 2 => () => new AuthorStructBuilder()
      case _ => StructBuilder.IgnoreAllFactory
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
      case 1 => this.title = StructBuilder.toString(fieldValue)
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

  override def build(): Seq[Byte] = items

  override def readItem(value: Any): Unit = {
    items += value.asInstanceOf[Byte]
  }
}

class BookStructWriter(book: Book) extends StructWriter {
  private val titleField = StructField(1, ThriftType.Binary)
  private val authorField = StructField(2, ThriftType.Struct)
  private val yearField = StructField(4, ThriftType.I32)
  private val pagesField = StructField(5, ThriftType.List)
  private val alternativesField = StructField(6, ThriftType.Map)

  override def fieldsToEncode: Seq[StructField] = {
    val fields = new mutable.ArrayBuffer[StructField](5)
    if (book.title != null) fields += titleField
    if (book.author != null) fields += authorField
    if (book.year != 0) fields += yearField
    if (book.pages != null) fields += pagesField
    if (book.alternatives != null) fields += alternativesField
    fields
  }

  def writeI8(fieldId: Short): Byte = ???
  def writeI16(fieldId: Short): Short = ???
  def writeI32(fieldId: Short): Int = fieldId match {
    case 4 => book.year
  }
  def writeI64(fieldId: Short): Long = ???
  def writeDouble(fieldId: Short): Double = ???
  def writeBoolean(fieldId: Short): Boolean = ???
  def writeBinary(fieldId: Short): Array[Byte] = fieldId match {
    case 1 => StructBuilder.toBinary(book.title)
  }
  def writeStruct(fieldId: Short): StructWriter = fieldId match {
    case 2 => new AuthorStructWriter(book.author)
  }
  def writeCollection(fieldId: Short): (ThriftType, Iterator[_]) = fieldId match {
    case 5 => (ThriftType.I8, book.pages.iterator)
  }
  def writeMap(fieldId: Short): (ThriftType, ThriftType, Iterator[(_, _)]) = fieldId match {
    case 6 => (ThriftType.Binary, ThriftType.Struct, book.alternatives.iterator)
  }

}

// --------------- BookService ---------------

trait BookService {
  def bookByTitle(title: String): Book

  def booksForAuthor(authorName: String): Seq[Book]
}

class BookServiceClient extends BookService {
  override def bookByTitle(title: String): Book = {
    ???
  }

  override def booksForAuthor(authorName: String): Seq[Book] = {
    ???
  }
}

object BookService {
  object BooksForAuthor {
    case class Args(authorName: String)

    class ArgsBuilder extends IgnoreAllStructBuilder {
      private[this] var authorName: String = _

      override def build(): Args = {
        // TODO: check for required values
        Args(authorName)
      }

      override def readBinary(fieldId: Short, fieldValue: Array[Byte]): Unit = {
        fieldId match {
          case 1 => this.authorName = StructBuilder.toString(fieldValue)
          case _ => ()
        }
      }
    }
  }
}

//class BookServiceServer(service: BookService, protocol: Protocol) {
//
//  def serviceDecoder(protocol: Protocol): Decoder[A] = {
//    val booksForAuthorArgsDecoder = protocol.structDecoder(() => new BookService.BooksForAuthor.ArgsBuilder())
//
//    //  booksForAuthorArgsDecoder.decode(buffer, readOffset)
//  }
//}
