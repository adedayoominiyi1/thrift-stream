package nl.grons.reactivethrift.decoders

import uk.co.real_logic.agrona.DirectBuffer

object DecoderUtil {

  /**
    * Concatenates the bytes in 'previousData' with bytes from 'newData' such that the result is 'needed' bytes
    * long (if possible).
    *
    * @param previousData the previously collected bytes
    * @param newData      the new data to concatenate to previousData
    * @param readOffset   the read offset in newData
    * @param needed       the desired size of the result
    * @return the concatenated data (size is between `previousData.length` and 'needed' bytes, both inclusive),
    *         and the new 'readOffset' in newData
    */
  def concatData(previousData: Array[Byte], newData: DirectBuffer, readOffset: Int, needed: Int): (Array[Byte], Int) = {
    val resultSize = Math.min(needed, previousData.length + newData.capacity() - readOffset)
    val concatenatedData = Array.ofDim[Byte](resultSize)
    System.arraycopy(previousData, 0, concatenatedData, 0, previousData.length)
    val bytesFromNewData = resultSize - previousData.length
    newData.getBytes(readOffset, concatenatedData, previousData.length, bytesFromNewData)
    (concatenatedData, readOffset + bytesFromNewData)
  }
}
