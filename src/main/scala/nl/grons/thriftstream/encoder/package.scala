package nl.grons.thriftstream

package object encoder {

  val VarInt16Encoder: Encoder[Short] =
    Encoder.map(_.toInt, VarInt32Encoder)

  val ZigZagInt32Encoder: Encoder[Int] = {
    @inline def intToZigZag(n: Int): Int = (n << 1) ^ (n >> 31)
    Encoder.map(intToZigZag, VarInt32Encoder)
  }

  val ZigZagInt16Encoder: Encoder[Short] =
    Encoder.map(_.toInt, ZigZagInt32Encoder)

  val ZigZagInt64Encoder: Encoder[Long] = {
    @inline def longToZigZag(n: Long): Long = (n << 1) ^ (n >> 63)
    Encoder.map(longToZigZag, VarInt64Encoder)
  }

  val DoubleEncoder: Encoder[Double] =
    Encoder.map(java.lang.Double.doubleToLongBits, Int64Encoder)

  val DoubleFastEncoder: Encoder[Double] =
    Encoder.map(java.lang.Double.doubleToLongBits, Int64FastEncoder)

}
