package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.decoders.DecodeResult.{DecodeFailure, DecodeInsufficientData, Decoded}
import nl.grons.reactivethrift.decoders.{DecodeResult, Decoder, Int16Decoder, Int8Decoder}
import nl.grons.reactivethrift.encoder.EncodeResult._
import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.prop.PropertyChecks
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class IntEncodersSpec extends FunSpec with PropertyChecks {
  describe("An int8 encoder/decoder") {
    it("can encode/decode at any position") {
      val byteGen = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)
      val startIndexGen: Gen[Int] = Gen.chooseNum(0, 100)
      forAll ((byteGen, "byte"), (startIndexGen, "startIndex")) { (byte: Byte, startIndex: Int) =>
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](startIndex + 1))
        Int8Encoder.encode(byte, buffer, startIndex) shouldBe Encoded(buffer, startIndex + 1)
        // Byte should appear at index startIndex:
        buffer.getByte(startIndex) shouldBe byte
        // All other bytes should still be 0:
        (0 to startIndex).filter(_ != startIndex).foreach { index =>
          buffer.getByte(index) shouldBe 0
        }
        // Decode at same index should read correct value:
        Int8Decoder.decode(buffer, startIndex) shouldBe Decoded(byte, buffer, startIndex + 1)
      }
    }

    it("can encode later when there is no space") {
      val buffer = new UnsafeBuffer(Array.ofDim[Byte](0))
      val encodeResult = Int8Encoder.encode(1, buffer, 0)
      encodeResult shouldBe an [EncodeInsufficientBuffer]
    }

    it("should decode what is encoded over multiple buffers") {
      val bytes: Gen[Byte] = Gen.chooseNum(Byte.MinValue, Byte.MaxValue)
      encodeDecodeTest[Byte](bytes, 1, Int8Encoder, Int8Decoder)
    }
  }

  describe("An int16 encoder/decoder") {
    it("should decode what is encoded when enough data is available") {
      forAll { short: Short =>
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](2))
        Int16Encoder.encode(short, buffer, 0) shouldBe Encoded(buffer, 2)
        buffer.getShort(0) shouldBe short
        Int16Decoder.decode(buffer, 0) shouldBe Decoded(short, buffer, 2)
      }
    }

    it("should decode what is encoded over multiple buffers") {
      val shorts: Gen[Short] = Gen.oneOf(Gen.oneOf[Short]((-1).toShort, Short.MaxValue), Gen.chooseNum[Short](Short.MinValue, Short.MaxValue))
      encodeDecodeTest[Short](shorts, 2, Int16Encoder, Int16Decoder)
    }
  }

  def encodeDecodeTest[A](values: Gen[A], minimumBufferSize: Int, encoder: Encoder[A], decoder: Decoder[A]): Unit = {
    def genBoundedList[T](maxSize: Int)(implicit g: Gen[T]): Gen[Seq[T]] = {
      Gen.choose(0, maxSize) flatMap { sz => Gen.listOfN(sz, g) }
    }

    /**
      * A seq of 1 to 10 buffer sizes. Each buffer is `0` to `minimumBufferSize * 2` long.
      * The last entry is always `minimumBufferSize`.
      */
    val bufferSizesGen: Gen[Seq[Int]] = genBoundedList(9)(Gen.chooseNum(0, minimumBufferSize * 2)).map(_ :+ minimumBufferSize)

    forAll((values, "values"), (bufferSizesGen, "bufferSizes")) { (value: A, bufferSizes: Seq[Int]) =>
      // The whenever protects against too aggressive shrinks.
      whenever(bufferSizes.sum >= minimumBufferSize) {
        val buffers = bufferSizes.map(size => new UnsafeBuffer(Array.ofDim[Byte](size))).toIndexedSeq

        // Encode
        var bufferIdx = 0
        var encodeResult = encoder.encode(value, buffers(bufferIdx), 0)
        encodeResult should not be a[EncodeFailure]
        var continuationEncoder: ContinuationEncoder = encodeResult match {
          case EncodeInsufficientBuffer(c) => c
          case _ => null
        }
        var done = encodeResult.isInstanceOf[Encoded] || bufferIdx == bufferSizes.length || continuationEncoder == null
        while (!done) {
          bufferIdx += 1
          encodeResult = continuationEncoder.encode(buffers(bufferIdx), 0)
          encodeResult should not be a[EncodeFailure]
          continuationEncoder = encodeResult match {
            case EncodeInsufficientBuffer(c) => c
            case _ => null
          }
          done = encodeResult.isInstanceOf[Encoded] || bufferIdx == bufferSizes.length || continuationEncoder == null
        }

        // Decode
        bufferIdx = 0
        done = false
        var decodeResult: DecodeResult[A] = null
        var nextDecoder: Decoder[A] = decoder
        do {
          decodeResult = nextDecoder.decode(buffers(bufferIdx), 0)
          decodeResult should not be a[DecodeFailure[_]]
          bufferIdx += 1
          nextDecoder = decodeResult match {
            case DecodeInsufficientData(cd) => cd
            case _ => null
          }
          done = decodeResult.isInstanceOf[Decoded[A]] || bufferIdx == bufferSizes.length || nextDecoder == null
        } while (!done)

        decodeResult shouldBe a[Decoded[_]]
        decodeResult.asInstanceOf[Decoded[A]].value shouldBe value
      }
    }
  }
}
