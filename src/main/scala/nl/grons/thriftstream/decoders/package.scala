package nl.grons.thriftstream

package object decoders {

  val VarInt16Decoder: Decoder[Short] =
    VarInt32Decoder.map(_.toShort)

  val ZigZagInt32Decoder: Decoder[Int] = {
    @inline def zigzagToInt(n: Int): Int = (n >>> 1) ^ - (n & 1)
    VarInt32Decoder.map(zigzagToInt)
  }

  val ZigZagInt16Decoder: Decoder[Short] =
    ZigZagInt32Decoder.map(_.toShort)

  val ZigZagInt64Decoder: Decoder[Long] = {
    @inline def zigzagToLong(n: Long): Long = (n >>> 1) ^ - (n & 1)
    VarInt64Decoder.map(zigzagToLong)
  }

  val DoubleDecoder: Decoder[Double] =
    Int64Decoder.map(java.lang.Double.longBitsToDouble)

  val DoubleFastDecoder: Decoder[Double] =
    Int64FastDecoder.map(java.lang.Double.longBitsToDouble)

}
