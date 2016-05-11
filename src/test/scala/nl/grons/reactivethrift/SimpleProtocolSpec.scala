package nl.grons.reactivethrift

import nl.grons.reactivethrift.example.{BookStructBuilder, Book}
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class SimpleProtocolSpec extends FunSpec {

  describe("SimpleProtocol") {
    it("can decode a struct") {
      val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))
      var writeIndex = 0
      buffer.putByte(writeIndex, 0x45); writeIndex += 1
      buffer.putInt(writeIndex, 2016); writeIndex += 4
      buffer.putInt(writeIndex, 0); writeIndex += 1

      val bookDecoder: Decoder[Book] = CompactProtocol.structDecoder(() => new BookStructBuilder())

      val result = bookDecoder.decode(buffer, 0)
      result shouldBe a [Decoded[_]]
      val bookResult = result.asInstanceOf[Decoded[Book]]
      bookResult.value.year shouldBe 2016
      bookResult.notConsumedBufferReadOffset shouldBe writeIndex
    }

  }

}
