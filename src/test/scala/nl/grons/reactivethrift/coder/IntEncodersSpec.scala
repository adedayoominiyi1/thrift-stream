package nl.grons.reactivethrift.coder

import java.nio.{ByteBuffer, ByteOrder}

import nl.grons.reactivethrift.decoders.DecodeResult._
import nl.grons.reactivethrift.decoders._
import nl.grons.reactivethrift.encoder.EncodeResult._
import nl.grons.reactivethrift.encoder._
import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.MutableDirectBuffer
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class IntEncodersSpec extends FunSpec with CoderSpecUtil {

  intCoderSpec[Byte](
    "Int8Coder",
    0xfe.toByte,
    1,
    Int8Encoder,
    Int8Decoder,
    Gen.chooseNum(Byte.MinValue, Byte.MaxValue),
    (buffer, index) => buffer.getByte(index)
  )

  intCoderSpec[Short](
    "Int16Coder",
    0xfedc.toShort,
    2,
    Int16Encoder,
    Int16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).getShort(index)
  )

  intCoderSpec[Int](
    "Int32Coder",
    0xfedcba98,
    4,
    Int32Encoder,
    Int32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).getInt(index)
  )

  intCoderSpec[Long](
    "Int64Coder",
    0xfedcba9876543210L,
    8,
    Int64Encoder,
    Int64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).getLong(index)
  )

  intCoderSpec[Short](
    "Int16FastCoder",
    0xfedc.toShort,
    2,
    Int16FastEncoder,
    Int16FastDecoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).order(ByteOrder.LITTLE_ENDIAN).getShort(index)
  )

  intCoderSpec[Int](
    "Int32FastCoder",
    0xfedcba98,
    4,
    Int32FastEncoder,
    Int32FastDecoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).order(ByteOrder.LITTLE_ENDIAN).getInt(index)
  )

  intCoderSpec[Long](
    "Int64FastCoder",
    0xfedcba9876543210L,
    8,
    Int64FastEncoder,
    Int64FastDecoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue),
    (buffer, index) => ByteBuffer.wrap(buffer.byteArray()).order(ByteOrder.LITTLE_ENDIAN).getLong(index)
  )

  def intCoderSpec[IntType](
                           coderName: String,
                           intValue: IntType,
                           intSerializationLength: Int,
                           intEncoder: Encoder[IntType],
                           intDecoder: Decoder[IntType],
                           intGen: Gen[IntType],
                           directIntRead: (MutableDirectBuffer, Int) => IntType
                         ): Unit = {

    describe(s"An $coderName") {
      it("can encode/decode at any position") {
        val startIndexGen: Gen[Int] = Gen.chooseNum(0, 80)
        forAll((intGen, "intValue"), (startIndexGen, "startIndex")) { (intValue: IntType, startIndex: Int) =>
          val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))

          // Encode at given index:
          intEncoder.encode(intValue, buffer, startIndex) shouldBe Encoded(buffer, startIndex + intSerializationLength)

          // Byte should appear at index startIndex:
          directIntRead(buffer, startIndex) shouldBe intValue

          // All other bytes should still be 0:
          val intIndexes = Range(startIndex, startIndex + intSerializationLength)
          Range(0, buffer.capacity())
            .filterNot(intIndexes.contains)
            .foreach { index =>
              buffer.getByte(index) shouldBe 0
            }

          // Decode at same index should read correct value:
          intDecoder.decode(buffer, startIndex) shouldBe Decoded(intValue, buffer, startIndex + intSerializationLength)
        }
      }

      it("can encode later when there is no space") {
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](0))
        val encodeResult = intEncoder.encode(intValue, buffer, 0)
        encodeResult shouldBe an[EncodeInsufficientBuffer]
      }

      it("should decode what is encoded over multiple buffers") {
        encodeDecodeTest[IntType](intGen, intSerializationLength, intEncoder, intDecoder)
      }
    }
  }

//  describe("regression int16") {
//    it("doesnt work yet") {
//      doEncodeDecodeTest[Short](-26555, List(2, 2), List(0, 1, 0, 1), Int16Encoder, Int16Decoder)
//    }
//  }

}
