package io.xmode.cahill.interview.data.generation

import org.apache.parquet.io.{OutputFile, PositionOutputStream}
import java.io.BufferedOutputStream
import java.io.IOException
import java.nio.file.Files

import java.nio.file.Path
import java.nio.file.StandardOpenOption.APPEND
import java.nio.file.StandardOpenOption.CREATE
import java.nio.file.StandardOpenOption.TRUNCATE_EXISTING

//Converted From https://github.com/tideworks/arvo2parquet/blob/master/src/main/java/com/tideworks/data_load/io/OutputFile.java
object OutputFileConverstion {
  private val IO_BUF_SIZE = 16 * 1024

  def nioPathToOutputFile(file: Path): OutputFile = { //noinspection ConstantConditions
    assert(file != null)
    new OutputFile() {
      @throws[IOException]
      override def create(blockSizeHint: Long): PositionOutputStream = makePositionOutputStream(file, IO_BUF_SIZE, trunc = false)

      @throws[IOException]
      override def createOrOverwrite(blockSizeHint: Long): PositionOutputStream = makePositionOutputStream(file, IO_BUF_SIZE, trunc = true)

      override def supportsBlockSize: Boolean = false

      override def defaultBlockSize: Long = 0
    }
  }

  @throws[IOException]
  private def makePositionOutputStream(file: Path, ioBufSize: Int, trunc: Boolean) = {
    val output = new BufferedOutputStream(Files.newOutputStream(file, CREATE, if (trunc) TRUNCATE_EXISTING else APPEND), ioBufSize)
    new PositionOutputStream() {
      private var position = 0

      @throws[IOException]
      override def write(b: Int): Unit = {
        output.write(b)
        position += 1
      }

      @throws[IOException]
      override def write(b: Array[Byte]): Unit = {
        output.write(b)
        position += b.length
      }

      @throws[IOException]
      override def write(b: Array[Byte], off: Int, len: Int): Unit = {
        output.write(b, off, len)
        position += len
      }

      @throws[IOException]
      override def flush(): Unit = {
        output.flush()
      }

      @throws[IOException]
      override def close(): Unit = {
        output.close()
      }

      @throws[IOException]
      override def getPos: Long = position
    }
  }
}
