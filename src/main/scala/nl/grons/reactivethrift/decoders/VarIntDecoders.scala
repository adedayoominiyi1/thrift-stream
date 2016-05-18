package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer

/**
  * Read an i32 from the wire as a varint. The MSB of each byte is set
  * if there is another byte to follow. This can read up to 5 bytes.
  */
object VarInt32Decoder extends Decoder[Int] {
  /** True when `b` has the most significant bit set, false when unset. */
  @inline private def msb(b: Byte): Boolean = (b & 0x80) == 0x80

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 5) {
      // All bytes are available, the fast path
      var result = 0
      var shift = 0
      var offset = readOffset
      var b: Byte = 0
      // read more bytes while the MSB is set
      // TODO: protect against reading more then 5 bytes!
      do {
        b = buffer.getByte(offset)
        offset += 1
        result |= (b & 0x7F) << shift
        shift += 7
      } while (msb(b))
      Decoded(result, buffer, offset)

    } else if (availableByteCount == 0) {
      // No bytes available at all
      DecodeInsufficientData(this)

    } else {
      // Some bytes available, the slow path...
      val (result, shift, complete, endOffset) = decodeVarInt32(0, 0, buffer, readOffset)
      if (complete) {
        Decoded(result, buffer, endOffset)
      } else {
        DecodeInsufficientData(new VarInt32ContinuationDecoder(result, shift))
      }
    }
  }

  private class VarInt32ContinuationDecoder(previousResult: Int, previousShift: Int) extends Decoder[Int] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
      val (result, shift, complete, endOffset) = decodeVarInt32(previousResult, previousShift, buffer, readOffset)
      if (complete) {
        Decoded(result, buffer, endOffset)
      } else {
        DecodeInsufficientData(new VarInt32ContinuationDecoder(result, shift))
      }
    }
  }

  private def decodeVarInt32(previousResult: Int, previousShift: Int, buffer: DirectBuffer, readOffset: Int): (Int, Int, Boolean, Int) = {
    val (msbBytes, followingBytes) = Range(readOffset, Math.min(buffer.capacity(), readOffset + 5))
      .map(buffer.getByte)
      .span(msb)
    val varIntBytes = msbBytes ++ followingBytes.take(1)
    val varIntComplete = followingBytes.nonEmpty
    val (nextResult, nextShift) = varIntBytes.foldLeft((previousResult, previousShift)) { case ((result, shift), b) =>
      (result | (b & 0x7F) << shift, shift + 7)
    }
    val endOffset = readOffset + varIntBytes.length
    (nextResult, nextShift, varIntComplete, endOffset)
  }
}

/**
  * Read an i64 from the wire as a varint. The MSB of each byte is set
  * if there is another byte to follow. This can read up to 10 bytes.
  */
object VarInt64Decoder extends Decoder[Long] {
  /** True when `b` has the most significant bit set, false when unset. */
  @inline private def msb(b: Byte): Boolean = (b & 0x80) == 0x80

  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 10) {
      // All bytes are available
      var result = 0L
      var shift = 0
      var offset = readOffset
      var b: Byte = 0
      // read more bytes while the MSB is set
      // TODO: protect against reading more then 10 bytes!
      do {
        b = buffer.getByte(offset)
        offset += 1
        result |= (b & 0x7F).toLong << shift
        shift += 7
      } while (msb(b))
      Decoded(result, buffer, offset)

    } else if (availableByteCount == 0) {
      // No bytes available at all
      DecodeInsufficientData(this)

    } else {
      // Some bytes available, the slow path...
      val (result, shift, complete, endOffset) = decodeVarInt64(0, 0, buffer, readOffset)
      if (complete) {
        Decoded(result, buffer, endOffset)
      } else {
        DecodeInsufficientData(new VarInt64ContinuationDecoder(result, shift))
      }
    }
  }

  private class VarInt64ContinuationDecoder(previousResult: Long, previousShift: Int) extends Decoder[Long] {
    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
      val (result, shift, complete, endOffset) = decodeVarInt64(previousResult, previousShift, buffer, readOffset)
      if (complete) {
        Decoded(result, buffer, endOffset)
      } else {
        DecodeInsufficientData(new VarInt64ContinuationDecoder(result, shift))
      }
    }
  }

  private def decodeVarInt64(previousResult: Long, previousShift: Int, buffer: DirectBuffer, readOffset: Int): (Long, Int, Boolean, Int) = {
    val (msbBytes, followingBytes) = Range(readOffset, Math.min(buffer.capacity(), readOffset + 10))
      .map(buffer.getByte)
      .span(msb)
    val varIntBytes = msbBytes ++ followingBytes.take(1)
    val varIntComplete = followingBytes.nonEmpty
    val (nextResult, nextShift) = varIntBytes.foldLeft((previousResult, previousShift)) { case ((result, shift), b) =>
      (result | (b & 0x7F).toLong << shift, shift + 7)
    }
    val endOffset = readOffset + varIntBytes.length
    (nextResult, nextShift, varIntComplete, endOffset)
  }
}
