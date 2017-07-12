package nl.grons.thriftstream

import nl.grons.thriftstream.decoders.Decoder

// Protocol

trait Protocol {
  def structDecoder[A](structBuilder: Factory[StructBuilder]): Decoder[A]
}

object Protocol {
  case class TMessage(name: String, messageType: Byte, sequenceId: Int) {
    require(messageType >= 1 && messageType <= 4)
  }
}

object MessageType {
  val Call: Byte = 1
  val Reply: Byte = 2
  val Exception: Byte = 3
  val OneWay: Byte = 4
}

trait ThriftType
object ThriftType {
  case object Bool extends ThriftType
  case object I8 extends ThriftType
  case object I16 extends ThriftType
  case object I32 extends ThriftType
  case object I64 extends ThriftType
  case object Double extends ThriftType
  case object Binary extends ThriftType
  case object List extends ThriftType
  case object Set extends ThriftType
  case object Map extends ThriftType
  case object Struct extends ThriftType
}

case class StructField(fieldId: Short, thriftType: ThriftType)
