Thrift-stream
===============

A proof-of-concept streaming implementation of the thrift protocol.

— An exploration of **continuation parsers and encoders** —

### TL DR

Continuation parsers and encoders try to decode (read)/encode (write) data _directly_ from/to a network buffer. When
the buffer has been fully read/written, it asks for more network buffers to continue.

### Goals

The idea is that we can use thrift-stream in high performance async environments like
[Finagle](https://twitter.github.io/finagle/), [Akka Streams](https://akka.io/docs) and
[Aeron](https://github.com/real-logic/aeron).

Requirements:

* single copy: from network buffer directly into entity, and from entity directly into network buffer
* immediate release of a network buffer, even when receiving a thrift message that spans multiple buffers
* no blocking
* construction of any entity (e.g. Java POJO's, Scala case classes)
* works equally well for framed as non-framed transport

These goals are conflicting when a traditional API is used. This library uses the novel idea of **continuation parsers**.

Decoding a network buffer is done by a parser. Offering a network buffer to such a parser either gives a decoded
entity or a _continuation parser_ (or a protocol error). The continuation parser contains a partially constructed
entity, and can be given more network buffers to complete decoding.

The API in short looks like this for receiving:

```scala
sealed abstract class DecodeResult[A]

object DecodeResult {
  case class Decoded[A](value: A, buffer: NetworkBuffer, nextReadOffset: Int) extends DecodeResult[A]
  case class DecodeFailure[A](error: String) extends DecodeResult[A]
  case class DecodeInsufficientData[A](continuationDecoder: Decoder[A]) extends DecodeResult[A]
}

trait Decoder[A] {
  def decode(buffer: NetworkBuffer, readOffset: Int): DecodeResult[A]
}
```

We can do something similar for sending data. We start by offering a value and a buffer to which it can be written. The
result is either that the full value was written (with the write offset for the next value), an error, or that the
network buffer is full. In the last case the returned continuation encoder knows exactly where to continue with encoding
the value to the next network buffer.

The API in short looks something like this:

```scala
sealed abstract class EncodeResult

trait ContinuationEncoder {
  def encode(buffer: MutableNetworkBuffer, writeOffset: Int): EncodeResult
}

object EncodeResult {
  case class Encoded(buffer: MutableNetworkBuffer, nextWriteOffset: Int) extends EncodeResult
  case class EncodeFailure(error: String) extends EncodeResult
  case class EncodeInsufficientBuffer(continuationEncoder: ContinuationEncoder) extends EncodeResult
}

trait Encoder[A] {
  def encode(value: A, buffer: MutableNetworkBuffer, writeOffset: Int): EncodeResult
}
```

With this in place it becomes possible to convert a stream of network buffers into a stream of decoded entities,
and a stream of entities into a stream of network buffers.

### Status

* fully working decoding of Thrift Compact Protocol
* entities are created by builders; these builders need to be generated from a thrift definitions, no code generation
  is in place
* decoding and encoding of Thrift message frames and all primitive types for the Thrift Compact Protocol and
  Thrift Binary Protocol

### About continuation parsers / encoders

A continuation parser works by building up state (in the entity builder) while recursing into the fields that need
to be decoded. At each step it validates if enough bytes are available in the input buffer. In case not enough bytes
are available, a new parser is constructed that references the entity builder and potentially some partially read data
(for example the first 2 bytes of a 4 byte integer). Of course, usually there are enough bytes to continue and things
progress very fast.

By using functional programming (parsers are monads) it is possible combine parsers (parsers use other parsers) without
too much programming overhead. See for example
https://github.com/erikvanoosten/thrift-stream/blob/master/src/main/scala/nl/grons/thriftstream/CompactProtocol.scala#L34
to see how a parser that decodes a var-int is combined with a byte array decoder.

Because recursion is essential, trampolining is used to prevent StackOverflowExceptions.

Although Scala is used here, this code can be translated into any programming language that supports lambdas or closures
(so that includes Java 8).

### Future work

To make this a fully functional thrift implementation the following needs to be done:

* write a code generation tool that generates the entity builders (for example with Scrooge templates), one could
  generate different builders: one for Apache Thrift generated classes, one for Scrooge generated Scala case classes
* work out encoding (which should be a simpler then decoding)
* perform compatibility tests

### Side effect: A Thrift protocol specification

In order to understand the Thrift protocols I had to dive into the code and create a specification. It is hosted here:
http://erikvanoosten.github.io/thrift-missing-specification


----

Copyright (C) 2016 - 2017 Erik van Oosten

This README is licensed under a <a rel="license" href="http://creativecommons.org/licenses/by/4.0/">Creative Commons Attribution 4.0 International License</a>.

This entire project is published under Apache Software License 2.0, see [LICENSE](LICENSE)
