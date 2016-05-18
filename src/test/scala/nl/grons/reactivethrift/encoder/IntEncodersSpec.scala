package nl.grons.reactivethrift.encoder

import nl.grons.reactivethrift.decoders.DecodeResult.{DecodeFailure, DecodeInsufficientData, Decoded}
import nl.grons.reactivethrift.decoders._
import nl.grons.reactivethrift.encoder.EncodeResult._
import org.scalacheck.Gen
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import org.scalatest.prop.PropertyChecks
import uk.co.real_logic.agrona.MutableDirectBuffer
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class IntEncodersSpec extends FunSpec with PropertyChecks {

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

  def encodeDecodeTest[A](values: Gen[A], minimumBufferSize: Int, encoder: Encoder[A], decoder: Decoder[A]): Unit = {
    def genBoundedList[T](maxSize: Int)(implicit g: Gen[T]): Gen[Seq[T]] = {
      Gen.choose(0, maxSize) flatMap { sz => Gen.listOfN(sz, g) }
    }

    // A seq of 1 to 10 buffer sizes. Each buffer is `0` to `minimumBufferSize * 2` long.
    // The last entry is always `minimumBufferSize`.
    val randomBufferSizesGen: Gen[Seq[Int]] = genBoundedList(9)(Gen.chooseNum(0, minimumBufferSize * 2)).map(_ :+ minimumBufferSize)

    val specialBufferSizesGen: Gen[Seq[Int]] = Gen.oneOf(
      // 20 times no bytes, then all bytes at once:
      Seq.fill(20)(0) :+ minimumBufferSize,
      // minimumBufferSize times 1 byte:
      Seq.fill(minimumBufferSize)(1),
      // minimumBufferSize times 1 byte interspersed with 0 bytes:
      Seq.tabulate(minimumBufferSize * 2)(s => s % 2),
      // minimumBufferSize times 2 bytes:
      Seq.fill(minimumBufferSize)(2)
    )

    val bufferSizesGen: Gen[Seq[Int]] = Gen.oneOf(specialBufferSizesGen, randomBufferSizesGen)

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
