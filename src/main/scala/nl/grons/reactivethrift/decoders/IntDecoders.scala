package nl.grons.reactivethrift.decoders

import nl.grons.reactivethrift.decoders.DecodeResult._
import uk.co.real_logic.agrona.DirectBuffer
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

object Int8Decoder extends Decoder[Byte] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Byte] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 1) {
      val value = buffer.getByte(readOffset)
      Decoded(value, buffer, readOffset + 1)
    } else {
      DecodeInsufficientData(this)
    }
  }
}

object Int16Decoder extends Decoder[Short] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 2) {
      val value = buffer.getShort(readOffset)
      Decoded(value, buffer, readOffset + 2)
    } else {
      val availableBytes = Array.ofDim[Byte](availableByteCount)
      buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
      DecodeInsufficientData(new Int16ContinuationDecoder(availableBytes))
    }
  }

  private class Int16ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Short] {
    require(previousData.length < 2)

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Short] = {
      val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 2)
      if (concatenatedData.length == 2) {
        val value = new UnsafeBuffer(concatenatedData).getShort(0)
        Decoded(value, buffer, newDataReadOffset)
      } else {
        DecodeInsufficientData(new Int16ContinuationDecoder(concatenatedData))
      }
    }
  }
}

object Int32Decoder extends Decoder[Int] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 4) {
      val value = buffer.getInt(readOffset)
      Decoded(value, buffer, readOffset + 4)
    } else {
      val availableBytes = Array.ofDim[Byte](availableByteCount)
      buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
      DecodeInsufficientData(new Int32ContinuationDecoder(availableBytes))
    }
  }

  private class Int32ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Int] {
    require(previousData.length < 4)

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Int] = {
      val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 4)
      if (concatenatedData.length == 4) {
        val value = new UnsafeBuffer(concatenatedData).getInt(0)
        Decoded(value, buffer, newDataReadOffset)
      } else {
        DecodeInsufficientData(new Int32ContinuationDecoder(concatenatedData))
      }
    }
  }
}

object Int64Decoder extends Decoder[Long] {
  override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
    val availableByteCount = buffer.capacity() - readOffset
    if (availableByteCount >= 8) {
      val value = buffer.getLong(readOffset)
      Decoded(value, buffer, readOffset + 8)
    } else {
      val availableBytes = Array.ofDim[Byte](availableByteCount)
      buffer.getBytes(readOffset, availableBytes, 0, availableByteCount)
      DecodeInsufficientData(new Int64ContinuationDecoder(availableBytes))
    }
  }

  private class Int64ContinuationDecoder(previousData: Array[Byte]) extends Decoder[Long] {
    require(previousData.length < 8)

    override def decode(buffer: DirectBuffer, readOffset: Int): DecodeResult[Long] = {
      val (concatenatedData, newDataReadOffset) = DecoderUtil.concatData(previousData, buffer, readOffset, 8)
      if (concatenatedData.length == 8) {
        val value = new UnsafeBuffer(concatenatedData).getLong(0)
        Decoded(value, buffer, newDataReadOffset)
      } else {
        DecodeInsufficientData(new Int64ContinuationDecoder(concatenatedData))
      }
    }
  }
}
