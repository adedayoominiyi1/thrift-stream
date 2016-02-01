package nl.grons.rethrift

import nl.grons.rethrift.example.{IBook, BookStructCoder}
import org.scalatest.FunSpec
import org.scalatest.Matchers._
import uk.co.real_logic.agrona.concurrent.UnsafeBuffer

class BlaSpec extends FunSpec {

  describe("Bla") {
    it("can decode a struct") {
      val buffer = new UnsafeBuffer(Array.ofDim[Byte](100))
      buffer.putInt(0, 4)
      buffer.putInt(4, 2016)
      buffer.putInt(8, 0)

      val result = new BookStructCoder().decode(buffer, 0)
      result shouldBe a [Decoded[_]]
      val bookResult = result.asInstanceOf[Decoded[IBook]]
      bookResult.value.year shouldBe 2016
      bookResult.notConsumedBufferReadOffset shouldBe 12
    }

  }

}
