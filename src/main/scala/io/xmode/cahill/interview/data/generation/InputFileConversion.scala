package io.xmode.cahill.interview.data.generation

import org.apache.parquet.io.{InputFile, SeekableInputStream}
import javax.annotation.Nonnull
import java.io.EOFException
import java.io.FileNotFoundException
import java.io.IOException
import java.io.RandomAccessFile
import java.nio.ByteBuffer
import java.nio.file.Path

//Converted From https://github.com/tideworks/arvo2parquet/blob/master/src/main/java/com/tideworks/data_load/io/InputFile.java
object InputFileConversion {
  private val COPY_BUFFER_SIZE = 8192

  @throws[FileNotFoundException]
  def nioPathToInputFile(@Nonnull file: Path): InputFile = { //noinspection ConstantConditions
    assert(file != null)
    val input = new RandomAccessFile(file.toFile, "r")
    new InputFile() {
      @throws[IOException]
      override def getLength: Long = input.length

      @throws[IOException]
      override def newStream: SeekableInputStream = new SeekableInputStream() {
        final private val tmpBuf = new Array[Byte](COPY_BUFFER_SIZE)
        private var markPos:Long = 0

        @throws[IOException]
        override def read: Int = input.read

        @SuppressWarnings(Array("NullableProblems"))
        @throws[IOException]
        override def read(b: Array[Byte]): Int = input.read(b)

        @SuppressWarnings(Array("NullableProblems"))
        @throws[IOException]
        override def read(b: Array[Byte], off: Int, len: Int): Int = input.read(b, off, len)

        @throws[IOException]
        override def skip(n: Long): Long = {
          val savPos = input.getFilePointer
          val amtLeft = input.length - savPos
          val nRecal = Math.min(n, amtLeft)
          val newPos = savPos + nRecal
          input.seek(newPos)
          val curPos = input.getFilePointer
          curPos - savPos
        }

        @throws[IOException]
        override def available: Int = 0

        @throws[IOException]
        override def close(): Unit = {
          input.close()
        }

        @SuppressWarnings(Array("unchecked", "unused", "UnusedReturnValue"))
        @throws[Throwable]
        private def uncheckedExceptionThrow[T <: Throwable, R](t: Throwable) = throw t.asInstanceOf[T]

        override def mark(readlimit: Int): Unit = {
          try
            markPos = input.getFilePointer
          catch {
            case e: IOException =>
              uncheckedExceptionThrow(e)
          }
        }

        @throws[IOException]
        override def reset(): Unit = {
          input.seek(markPos)
        }

        override

        def markSupported: Boolean = true

        @throws[IOException]
        override def getPos: Long = input.getFilePointer

        @throws[IOException]
        override def seek(l: Long): Unit = {
          input.seek(l)
        }

        @throws[IOException]
        override def readFully(bytes: Array[Byte]): Unit = {
          input.readFully(bytes)
        }

        @throws[IOException]
        override def readFully(bytes: Array[Byte], i: Int, i1: Int): Unit = {
          input.readFully(bytes, i, i1)
        }

        @throws[IOException]
        override def read(byteBuffer: ByteBuffer): Int = readDirectBuffer(byteBuffer, tmpBuf, input)

        @throws[IOException]
        override def readFully(byteBuffer: ByteBuffer): Unit = {
          readFullyDirectBuffer(byteBuffer, tmpBuf, input)
        }
      }
    }
  }

  @FunctionalInterface
  private trait ByteBufReader {
    @throws[IOException]
    def read(b: Array[Byte], off: Int, len: Int): Int
  }

  @throws[IOException]
  private def readDirectBuffer(byteBufr: ByteBuffer, tmpBuf: Array[Byte], rdr:RandomAccessFile) = { // copy all the bytes that return immediately, stopping at the first
    // read that doesn't return a full buffer.
    var nextReadLength = Math.min(byteBufr.remaining, tmpBuf.length)
    var totalBytesRead = nextReadLength
    var bytesRead = rdr.read(tmpBuf, 0, nextReadLength)
    do {
      byteBufr.put(tmpBuf)
      totalBytesRead += bytesRead
      nextReadLength = Math.min(byteBufr.remaining, tmpBuf.length)
    } while (nextReadLength > 0 && bytesRead >= 0)

    if (bytesRead < 0) { // return -1 if nothing was read
      if (totalBytesRead == 0) -1
      else totalBytesRead
    }
    else { // copy the last partial buffer
      byteBufr.put(tmpBuf, 0, bytesRead)
      totalBytesRead += bytesRead
      totalBytesRead
    }
  }

  @throws[IOException]
  private def readFullyDirectBuffer(byteBufr: ByteBuffer, tmpBuf: Array[Byte], rdr: RandomAccessFile): Unit = {
    var nextReadLength = Math.min(byteBufr.remaining, tmpBuf.length)
    var bytesRead = rdr.read(tmpBuf, 0, nextReadLength)
    do {
      byteBufr.put(tmpBuf, 0, bytesRead)
      nextReadLength = Math.min(byteBufr.remaining, tmpBuf.length)
      bytesRead = rdr.read(tmpBuf, 0, nextReadLength)
    } while (nextReadLength > 0 && bytesRead >= 0)
    if (bytesRead < 0 && byteBufr.remaining > 0) throw new EOFException("Reached the end of stream with " + byteBufr.remaining + " bytes left to read")
  }
}
