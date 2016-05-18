package nl.grons.reactivethrift.coder

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
    1.toByte,
    1,
    Int8Encoder,
    Int8Decoder,
    Gen.chooseNum(Byte.MinValue, Byte.MaxValue),
    (buffer, index) => buffer.getByte(index)
  )

  intCoderSpec[Short](
    1.toShort,
    2,
    Int16Encoder,
    Int16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue),
    (buffer, index) => buffer.getShort(index)
  )

  intCoderSpec[Int](
    1,
    4,
    Int32Encoder,
    Int32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue),
    (buffer, index) => buffer.getInt(index)
  )

  intCoderSpec[Long](
    1L,
    8,
    Int64Encoder,
    Int64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue),
    (buffer, index) => buffer.getLong(index)
  )

  def intCoderSpec[IntType](
                             intValue: IntType,
                             intSerializationLength: Int,
                             intEncoder: Encoder[IntType],
                             intDecoder: Decoder[IntType],
                             intGen: Gen[IntType],
                             directIntRead: (MutableDirectBuffer, Int) => IntType
                           ): Unit = {

    describe(s"An ${intEncoder.getClass.getSimpleName} encoder/decoder") {
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

}
