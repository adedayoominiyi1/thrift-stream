package nl.grons.reactivethrift.coder

import nl.grons.reactivethrift.decoders.DecodeResult._
import nl.grons.reactivethrift.decoders._
import nl.grons.reactivethrift.encoder.EncodeResult._
import nl.grons.reactivethrift.encoder._
import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class VarIntEncodersSpec extends FunSpec with CoderSpecUtil {

  varIntCoderSpec[Short](
    "VarInt16",
    1.toShort,
    5,
    VarInt16Encoder,
    VarInt16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue)
  )

  varIntCoderSpec[Int](
    "VarInt32",
    1,
    5,
    VarInt32Encoder,
    VarInt32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue)
  )

  varIntCoderSpec[Long](
    "VarInt64",
    1L,
    10,
    VarInt64Encoder,
    VarInt64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue)
  )

  varIntCoderSpec[Short](
    "ZigZagInt16",
    1.toShort,
    5,
    ZigZagInt16Encoder,
    ZigZagInt16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue)
  )

  varIntCoderSpec[Int](
    "ZigZagInt32",
    1,
    5,
    ZigZagInt32Encoder,
    ZigZagInt32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue)
  )

  varIntCoderSpec[Long](
    "ZigZagInt64",
    1L,
    10,
    ZigZagInt64Encoder,
    ZigZagInt64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue)
  )

  def varIntCoderSpec[IntType](
                              encoderName: String,
                              intValue: IntType,
                              maxSerializationLength: Int,
                              intEncoder: Encoder[IntType],
                              intDecoder: Decoder[IntType],
                              intGen: Gen[IntType]
                           ): Unit = {

    describe(s"An $encoderName encoder/decoder") {
      it("can encode/decode at any position") {
        val startIndexGen: Gen[Int] = Gen.chooseNum(0, 80)
        forAll((intGen, "intValue"), (startIndexGen, "startIndex")) { (intValue: IntType, startIndex: Int) =>
          val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))

          // Encode at given index:
          val encodeResult = intEncoder.encode(intValue, buffer, startIndex)
          encodeResult shouldBe an [Encoded]
          encodeResult.asInstanceOf[Encoded].buffer shouldBe buffer
          encodeResult.asInstanceOf[Encoded].nextWriteOffset should (be > startIndex and be <= (startIndex + maxSerializationLength))

          // All other bytes should still be 0:
          val intIndexes = Range(startIndex, startIndex + maxSerializationLength)
          Range(0, buffer.capacity())
            .filterNot(intIndexes.contains)
            .foreach { index =>
              buffer.getByte(index) shouldBe 0
            }

          // Decode at same index should read correct value:
          val decodeResult = intDecoder.decode(buffer, startIndex)
          decodeResult shouldBe a [Decoded[_]]
          decodeResult.asInstanceOf[Decoded[IntType]].value shouldBe intValue
          decodeResult.asInstanceOf[Decoded[IntType]].buffer shouldBe buffer
          decodeResult.asInstanceOf[Decoded[IntType]].nextReadOffset should be > startIndex
          decodeResult.asInstanceOf[Decoded[IntType]].nextReadOffset should be <= (startIndex + maxSerializationLength)
        }
      }

      it("can encode later when there is no space") {
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](0))
        val encodeResult = intEncoder.encode(intValue, buffer, 0)
        encodeResult shouldBe an [EncodeInsufficientBuffer]
      }

      it("should decode what is encoded over multiple buffers") {
        encodeDecodeTest[IntType](intGen, maxSerializationLength, intEncoder, intDecoder)
      }
    }
  }

}
