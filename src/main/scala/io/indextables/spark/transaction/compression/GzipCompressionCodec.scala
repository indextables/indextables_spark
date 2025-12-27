package io.indextables.spark.transaction.compression

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

/**
 * GZIP compression codec for transaction logs.
 *
 * GZIP provides excellent compression ratios (3-5x) with minimal CPU overhead, making it ideal for transaction log
 * compression where download time is critical. Uses Java standard library - no external dependencies required.
 *
 * @param level
 *   GZIP compression level (0-9, default 6) 0 = no compression, 1 = fastest, 6 = default, 9 = best compression
 */
class GzipCompressionCodec(level: Int = 6) extends CompressionCodec {
  require(level >= 0 && level <= 9, s"GZIP compression level must be 0-9, got: $level")

  override def codecByte: Byte = 0x01
  override def name: String    = "gzip"

  override def compress(input: InputStream, output: OutputStream): Unit = {
    val gzipOut = new GZIPOutputStream(output) {
      // Set custom compression level
      `def`.setLevel(level)
    }
    try {
      val buffer    = new Array[Byte](8192)
      var bytesRead = 0
      while ({ bytesRead = input.read(buffer); bytesRead != -1 })
        gzipOut.write(buffer, 0, bytesRead)
      gzipOut.flush()
    } finally
      gzipOut.close()
  }

  override def decompress(input: InputStream, output: OutputStream): Unit = {
    val gzipIn = new GZIPInputStream(input)
    try {
      val buffer    = new Array[Byte](8192)
      var bytesRead = 0
      while ({ bytesRead = gzipIn.read(buffer); bytesRead != -1 })
        output.write(buffer, 0, bytesRead)
      output.flush()
    } finally
      gzipIn.close()
  }

  override def compress(data: Array[Byte]): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    val gzipOut = new GZIPOutputStream(baos) {
      `def`.setLevel(level)
    }
    try {
      gzipOut.write(data)
      gzipOut.flush()
    } finally
      gzipOut.close()
    baos.toByteArray
  }

  override def decompress(data: Array[Byte]): Array[Byte] = {
    val bais   = new ByteArrayInputStream(data)
    val gzipIn = new GZIPInputStream(bais)
    val baos   = new ByteArrayOutputStream()
    try {
      val buffer    = new Array[Byte](8192)
      var bytesRead = 0
      while ({ bytesRead = gzipIn.read(buffer); bytesRead != -1 })
        baos.write(buffer, 0, bytesRead)
      baos.toByteArray
    } finally
      gzipIn.close()
  }
}
