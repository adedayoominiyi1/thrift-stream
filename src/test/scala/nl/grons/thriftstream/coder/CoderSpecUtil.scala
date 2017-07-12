package nl.grons.thriftstream.coder

import nl.grons.thriftstream.decoders.DecodeResult.{DecodeFailure, DecodeInsufficientData, Decoded}
import nl.grons.thriftstream.decoders.{DecodeResult, Decoder}
import nl.grons.thriftstream.encoder.EncodeResult.{ContinuationEncoder, EncodeFailure, EncodeInsufficientBuffer, Encoded}
import nl.grons.thriftstream.encoder.Encoder
import org.scalacheck.Gen
import org.scalatest.Matchers._
import org.scalatest.prop.PropertyChecks
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

trait CoderSpecUtil extends PropertyChecks {

  /**
    * Tests that values that can be encoded can also be decoded.
    * Encoding and decoding takes place over multiple buffers so that continuation encoders/decoders are also tested.
    */
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

    forAll(
      (values, "values"),
      (bufferSizesGen, "encodeBufferSizes"),
      (bufferSizesGen, "decodeBufferSizes")
    ) { (value: A, encodeBufferSizes: Seq[Int], decodeBufferSizes: Seq[Int]) =>
      // The whenever protects against too aggressive shrinks.
      whenever(encodeBufferSizes.sum >= minimumBufferSize && decodeBufferSizes.sum >= minimumBufferSize) {
        doEncodeDecodeTest(value, encodeBufferSizes, decodeBufferSizes, encoder, decoder)
      }
    }
  }

  def doEncodeDecodeTest[A](value: A, encodeBufferSizes: Seq[Int], decodeBufferSizes: Seq[Int], encoder: Encoder[A], decoder: Decoder[A]): Unit = {
    val encodeBuffers = encodeBufferSizes.map(size => new UnsafeBuffer(Array.ofDim[Byte](size))).toIndexedSeq

    // Encode
    var bufferIdx = 0
    var encodeResult = encoder.encode(value, encodeBuffers(bufferIdx), 0)
    encodeResult should not be a[EncodeFailure]
    var continuationEncoder: ContinuationEncoder = encodeResult match {
      case EncodeInsufficientBuffer(c) => c
      case _ => null
    }
    var done = encodeResult.isInstanceOf[Encoded] || bufferIdx == encodeBufferSizes.length || continuationEncoder == null
    while (!done) {
      bufferIdx += 1
      encodeResult = continuationEncoder.encode(encodeBuffers(bufferIdx), 0)
      encodeResult should not be a[EncodeFailure]
      continuationEncoder = encodeResult match {
        case EncodeInsufficientBuffer(c) => c
        case _ => null
      }
      done = encodeResult.isInstanceOf[Encoded] || bufferIdx == encodeBufferSizes.length || continuationEncoder == null
    }

    // Copy data from encode buffers to decode buffers
    Console.out.println("value: " + value + " encode: " + encodeBuffers.map(_.byteArray().mkString("[", ",", "]")).mkString(" - "))
    val encodedBytes = encodeBuffers.map(_.byteArray()).reduce(_ ++ _)
    val decodeBufferSlices = decodeBufferSizes.scanLeft(0)(_ + _).sliding(2)
    val decodeBuffers = decodeBufferSlices.map { case Seq(start, end) => new UnsafeBuffer(encodedBytes.slice(start, end))}.toIndexedSeq
    Console.out.println("value: " + value + " decode: " + decodeBuffers.map(_.byteArray().mkString("[", ",", "]")).mkString(" - "))

    // Decode
    bufferIdx = 0
    done = false
    var decodeResult: DecodeResult[A] = null
    var nextDecoder: Decoder[A] = decoder
    do {
      decodeResult = nextDecoder.decode(decodeBuffers(bufferIdx), 0)
      decodeResult should not be a[DecodeFailure[_]]
      bufferIdx += 1
      nextDecoder = decodeResult match {
        case DecodeInsufficientData(cd) => cd
        case _ => null
      }
      done = decodeResult.isInstanceOf[Decoded[A]] || bufferIdx == decodeBufferSizes.length || nextDecoder == null
    } while (!done)

    decodeResult shouldBe a [Decoded[_]]
    decodeResult.asInstanceOf[Decoded[A]].value shouldBe value
  }


}
