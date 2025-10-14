package io.indextables.spark.transaction.compression

import java.io.{InputStream, OutputStream}

/**
 * Abstraction for compression codecs used in transaction logs.
 *
 * Provides both stream-based and byte array-based compression/decompression
 * to support different use cases efficiently.
 */
trait CompressionCodec {
  /** Codec identifier byte (0x01 for GZIP, etc.) */
  def codecByte: Byte

  /** Codec name for configuration */
  def name: String

  /** Compress data from input stream to output stream */
  def compress(input: InputStream, output: OutputStream): Unit

  /** Decompress data from input stream to output stream */
  def decompress(input: InputStream, output: OutputStream): Unit

  /** Compress byte array */
  def compress(data: Array[Byte]): Array[Byte]

  /** Decompress byte array */
  def decompress(data: Array[Byte]): Array[Byte]
}
