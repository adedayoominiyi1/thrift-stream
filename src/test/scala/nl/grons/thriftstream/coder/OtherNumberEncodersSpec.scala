package nl.grons.thriftstream.coder

import nl.grons.thriftstream.decoders.DecodeResult._
import nl.grons.thriftstream.decoders._
import nl.grons.thriftstream.encoder.EncodeResult._
import nl.grons.thriftstream.encoder._
import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class OtherNumberEncodersSpec extends FunSpec with CoderSpecUtil {

  numberCoderSpec[Short](
    "VarInt16",
    1.toShort,
    5,
    VarInt16Encoder,
    VarInt16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue)
  )

  numberCoderSpec[Int](
    "VarInt32",
    1,
    5,
    VarInt32Encoder,
    VarInt32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue)
  )

  numberCoderSpec[Long](
    "VarInt64",
    1L,
    10,
    VarInt64Encoder,
    VarInt64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue)
  )

  numberCoderSpec[Short](
    "ZigZagInt16",
    1.toShort,
    5,
    ZigZagInt16Encoder,
    ZigZagInt16Decoder,
    Gen.chooseNum(Short.MinValue, Short.MaxValue)
  )

  numberCoderSpec[Int](
    "ZigZagInt32",
    1,
    5,
    ZigZagInt32Encoder,
    ZigZagInt32Decoder,
    Gen.chooseNum(Int.MinValue, Int.MaxValue)
  )

  numberCoderSpec[Long](
    "ZigZagInt64",
    1L,
    10,
    ZigZagInt64Encoder,
    ZigZagInt64Decoder,
    Gen.chooseNum(Long.MinValue, Long.MaxValue)
  )

  numberCoderSpec[Double](
    "DoubleEncoder",
    0.1D,
    8,
    DoubleEncoder,
    DoubleDecoder,
    Gen.chooseNum(Double.MinValue, Double.MaxValue)
  )

  numberCoderSpec[Double](
    "DoubleFastEncoder",
    0.1D,
    8,
    DoubleFastEncoder,
    DoubleFastDecoder,
    Gen.chooseNum(Double.MinValue, Double.MaxValue)
  )

  def numberCoderSpec[NumberType](
                                   coderName: String,
                                   value: NumberType,
                                   maxSerializationLength: Int,
                                   intEncoder: Encoder[NumberType],
                                   intDecoder: Decoder[NumberType],
                                   intGen: Gen[NumberType]
  ): Unit = {

    describe(s"A $coderName") {
      it("can encode/decode at any position") {
        val startIndexGen: Gen[Int] = Gen.chooseNum(0, 80)
        forAll((intGen, "intValue"), (startIndexGen, "startIndex")) { (value: NumberType, startIndex: Int) =>
          val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))

          // Encode at given index:
          val encodeResult = intEncoder.encode(value, buffer, startIndex)
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
          decodeResult.asInstanceOf[Decoded[NumberType]].value shouldBe value
          decodeResult.asInstanceOf[Decoded[NumberType]].buffer shouldBe buffer
          decodeResult.asInstanceOf[Decoded[NumberType]].nextReadOffset should be > startIndex
          decodeResult.asInstanceOf[Decoded[NumberType]].nextReadOffset should be <= (startIndex + maxSerializationLength)
        }
      }

      it("can encode later when there is no space") {
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](0))
        val encodeResult = intEncoder.encode(value, buffer, 0)
        encodeResult shouldBe an [EncodeInsufficientBuffer]
      }

      it("should decode what is encoded over multiple buffers") {
        encodeDecodeTest[NumberType](intGen, maxSerializationLength, intEncoder, intDecoder)
      }
    }
  }

}
