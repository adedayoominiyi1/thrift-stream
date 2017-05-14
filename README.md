Thrift-stream
===============

A proof-of-concept streaming implementation of the thrift protocol.

### Goals

The idea is that we can use thrift-stream in high performance async environments like Finagle or Akka Streams.
For this we desire:

* single copy: from network buffer directly into entity, and from entity directly into network buffer
* immediate release of a network buffer, even when receiving a thrift message that spans multiple buffers
* no blocking
* construction of any entity (e.g. Java POJO's, Scala case classes)
* works equally well for framed as non-framed transport

These goals are conflicting when a traditional API is used. This library uses the novel idea of **continuation parsers**.

Decoding a network buffer is done by a parser. Offering a network buffer to such a parser either gives a decoded entity or a _continuation parser_ (or a protocol error). The continuation parser contains a partially constructed entity, and can be given more network buffers to complete decoding.

With this in place it becomes possible to convert a stream of network buffers into a stream of decoded entities, and a stream of entities into a stream of network buffers.

### Status

* fully working decoding of Thrift Compact Protocol
* entities are created by builders, these builders need to be generated, there is no code generation
* decoding and encoding of all primitive types for the Thrift Compact Protocol and Thrift Binary Protocol

### About continuation parsers

A continuation parser works by building up state (in the entity builder) while recursing into the fields that need to be decoded. At each step it validates if enough bytes are available in the input buffer. In case not enough bytes are available, a new parser is contructed that references the entity builder and potentially some partially read data (for example the first 2 bytes of a 4 byte integer).

By using functional programming (parsers are monads) it is possible combine parsers (parsers use other parsers) without too much programming overhead. See for example https://github.com/erikvanoosten/thrift-stream/blob/master/src/main/scala/nl/grons/reactivethrift/CompactProtocol.scala#L34 to see how a parser that decodes a var-int is combined with a byte array decoder.

Because recursion is essential, trampolining is used to prevent StackOverflowExceptions.

Although Scala is used here, this code can be translated into any programming language that supports closures (so that includes Java 8).

### Future work

To make this a fully functional thrift implementation the following needs to be done:

* write a code generation tool that generates the enitity builders (for example with Scrooge templates), one could generate different builders: one for Apache Thrift generated classes, one for Scrooge generated Scala case classes
* work out encoding (which should be a simpler then decoding)
* perform compatibility tests

### Side effect: A Thrift protocol specification

In order to understand the Thrift protocols I had to dive into the code and create a specification. It is hosted here: http://erikvanoosten.github.io/thrift-missing-specification


----

Copyright (c) 2016 Erik van Oosten

Published under Apache Software License 2.0, see [LICENSE](LICENSE)
