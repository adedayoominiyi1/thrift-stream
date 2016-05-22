package nl.grons.reactivethrift

import nl.grons.reactivethrift.decoders.DecodeResult.Decoded
import nl.grons.reactivethrift.decoders.Decoder
import nl.grons.reactivethrift.example.{Book, BookStructBuilder}
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class CompactProtocolSpec extends FunSpec {

  describe("CompactProtocol") {
    it("can decode a struct") {
      val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))
      var writeIndex = 0
      // Start of ZigZagInt field (0x45)
      buffer.putByte(writeIndex, 0x45); writeIndex += 1
      // Zig zag int with value 2016
      val year = 2016
      val yearAsZigZagInt = (year << 1) ^ (year >> 31)
      buffer.putByte(writeIndex, (((yearAsZigZagInt & 0x7f) | 0x80) & 0xff).toByte); writeIndex += 1
      buffer.putByte(writeIndex, (yearAsZigZagInt >>> 7 & 0xff).toByte); writeIndex += 1
      // Stop field (0)
      buffer.putInt(writeIndex, 0); writeIndex += 1

      val bookDecoder: Decoder[Book] = CompactProtocol.structDecoder(() => new BookStructBuilder())

      val result = bookDecoder.decode(buffer, 0)
      result shouldBe a [Decoded[_]]
      val bookResult = result.asInstanceOf[Decoded[Book]]
      bookResult.value.year shouldBe year
      bookResult.nextReadOffset shouldBe writeIndex
    }

  }

}
