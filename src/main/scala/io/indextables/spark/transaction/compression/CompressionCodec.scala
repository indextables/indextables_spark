package io.indextables.spark.transaction.compression

import java.io.{InputStream, OutputStream}

/**
 * Abstraction for compression codecs used in transaction logs.
 *
 * Provides both stream-based and byte array-based compression/decompression to support different use cases efficiently.
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

  /**
   * Create a compressing output stream wrapper.
   *
   * Data written to the returned stream will be compressed before being written to the underlying output stream.
   *
   * @param output
   *   The underlying output stream to write compressed data to
   * @return
   *   An OutputStream that compresses data before writing
   */
  def createCompressingOutputStream(output: OutputStream): OutputStream

  /**
   * Create a decompressing input stream wrapper.
   *
   * Data read from the returned stream will be decompressed from the underlying input stream.
   *
   * @param input
   *   The underlying input stream containing compressed data
   * @return
   *   An InputStream that decompresses data while reading
   */
  def createDecompressingInputStream(input: InputStream): InputStream
}
