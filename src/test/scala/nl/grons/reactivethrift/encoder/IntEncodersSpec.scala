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
                             maxSerializationLength: Int,
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
          intEncoder.encode(intValue, buffer, startIndex) shouldBe Encoded(buffer, startIndex + maxSerializationLength)

          // Byte should appear at index startIndex:
          directIntRead(buffer, startIndex) shouldBe intValue

          // All other bytes should still be 0:
          val intIndexes = Range(startIndex, startIndex + maxSerializationLength)
          Range(0, buffer.capacity())
            .filterNot(intIndexes.contains)
            .foreach { index =>
              buffer.getByte(index) shouldBe 0
            }

          // Decode at same index should read correct value:
          intDecoder.decode(buffer, startIndex) shouldBe Decoded(intValue, buffer, startIndex + maxSerializationLength)
        }
      }

      it("can encode later when there is no space") {
        val buffer = new UnsafeBuffer(Array.ofDim[Byte](0))
        val encodeResult = intEncoder.encode(intValue, buffer, 0)
        encodeResult shouldBe an[EncodeInsufficientBuffer]
      }

      it("should decode what is encoded over multiple buffers") {
        def genBoundedList[T](maxSize: Int)(implicit g: Gen[T]): Gen[Seq[T]] = {
          Gen.choose(0, maxSize) flatMap { sz => Gen.listOfN(sz, g) }
        }

        /**
          * A seq of 1 to 10 buffer sizes. Each buffer is `0` to `intSerializationLength * 2` long.
          * The last entry is always `intSerializationLength`.
          */
        val bufferSizesGen: Gen[Seq[Int]] = genBoundedList(9)(Gen.chooseNum(0, maxSerializationLength * 2)).map(_ :+ maxSerializationLength)

        forAll((intGen, "values"), (bufferSizesGen, "bufferSizes")) { (value: IntType, bufferSizes: Seq[Int]) =>
          // The whenever protects against too aggressive shrinks.
          whenever(bufferSizes.sum >= maxSerializationLength) {
            val buffers = bufferSizes.map(size => new UnsafeBuffer(Array.ofDim[Byte](size))).toIndexedSeq

            // Encode
            var bufferIdx = 0
            var encodeResult = intEncoder.encode(value, buffers(bufferIdx), 0)
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
            var decodeResult: DecodeResult[IntType] = null
            var nextDecoder: Decoder[IntType] = intDecoder
            do {
              decodeResult = nextDecoder.decode(buffers(bufferIdx), 0)
              decodeResult should not be a[DecodeFailure[_]]
              bufferIdx += 1
              nextDecoder = decodeResult match {
                case DecodeInsufficientData(cd) => cd
                case _ => null
              }
              done = decodeResult.isInstanceOf[Decoded[IntType]] || bufferIdx == bufferSizes.length || nextDecoder == null
            } while (!done)

            decodeResult shouldBe a[Decoded[_]]
            decodeResult.asInstanceOf[Decoded[IntType]].value shouldBe value
          }
        }
      }
    }
  }
}
