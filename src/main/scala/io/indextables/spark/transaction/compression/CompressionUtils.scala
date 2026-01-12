package io.indextables.spark.transaction.compression

import java.io.{FilterOutputStream, InputStream, OutputStream, PushbackInputStream}

/**
 * Utilities for reading/writing compressed transaction log files.
 *
 * Provides automatic compression detection and transparent compression/decompression for transaction log and checkpoint
 * files.
 */
object CompressionUtils {

  /** Magic byte indicating compressed file format */
  val MAGIC_BYTE: Byte = 0x01

  // Codec registry mapping codec bytes to implementations
  private val codecs: Map[Byte, CompressionCodec] = Map(
    0x01.toByte -> new GzipCompressionCodec()
  )

  /**
   * Read transaction log file with automatic compression detection.
   *
   * @param data
   *   Raw file bytes
   * @return
   *   Decompressed JSON bytes
   * @throws IllegalArgumentException
   *   if file is empty or compressed format is invalid
   * @throws UnsupportedOperationException
   *   if compression codec is unknown
   */
  def readTransactionFile(data: Array[Byte]): Array[Byte] = {
    if (data.isEmpty) {
      throw new IllegalArgumentException("Transaction file is empty")
    }

    // Check for magic byte indicating compression
    if (data(0) == MAGIC_BYTE) {
      // Compressed format
      if (data.length < 2) {
        throw new IllegalArgumentException(
          "Compressed file too short (missing codec byte)"
        )
      }

      val codecByte = data(1)
      val codec = codecs.getOrElse(
        codecByte,
        throw new UnsupportedOperationException(
          s"Unknown compression codec: 0x${Integer.toHexString(codecByte & 0xff)}. " +
            s"Supported codecs: ${codecs.keys.map(b => s"0x${Integer.toHexString(b & 0xff)}").mkString(", ")}"
        )
      )

      // Decompress data (skip magic byte + codec byte header)
      val compressedData = data.slice(2, data.length)
      codec.decompress(compressedData)

    } else {
      // Legacy uncompressed format
      data
    }
  }

  /**
   * Write transaction log file with optional compression.
   *
   * @param jsonData
   *   JSON bytes to write
   * @param codec
   *   Optional compression codec (None = uncompressed)
   * @return
   *   File bytes with compression headers if applicable
   */
  def writeTransactionFile(jsonData: Array[Byte], codec: Option[CompressionCodec]): Array[Byte] =
    codec match {
      case Some(c) =>
        // Compress data
        val compressed = c.compress(jsonData)

        // Build file: magic byte + codec byte + compressed data
        val output = new Array[Byte](2 + compressed.length)
        output(0) = MAGIC_BYTE
        output(1) = c.codecByte
        System.arraycopy(compressed, 0, output, 2, compressed.length)
        output

      case None =>
        // Uncompressed legacy format
        jsonData
    }

  /**
   * Get codec from configuration.
   *
   * @param compressionEnabled
   *   Whether compression is enabled
   * @param codecName
   *   Codec name ("gzip", "none")
   * @param gzipLevel
   *   GZIP compression level (0-9)
   * @return
   *   Optional compression codec
   * @throws IllegalArgumentException
   *   if codec name is unknown
   */
  def getCodec(
    compressionEnabled: Boolean,
    codecName: String,
    gzipLevel: Int = 6
  ): Option[CompressionCodec] =
    if (!compressionEnabled || codecName.equalsIgnoreCase("none")) {
      None
    } else if (codecName.equalsIgnoreCase("gzip")) {
      Some(new GzipCompressionCodec(gzipLevel))
    } else {
      throw new IllegalArgumentException(
        s"Unknown compression codec: $codecName (supported: gzip, none)"
      )
    }

  /**
   * Check if file data is compressed.
   *
   * @param data
   *   File bytes
   * @return
   *   true if file is compressed (starts with magic byte)
   */
  def isCompressed(data: Array[Byte]): Boolean =
    data.nonEmpty && data(0) == MAGIC_BYTE

  /**
   * Get compression codec from file data.
   *
   * @param data
   *   File bytes
   * @return
   *   Optional compression codec if file is compressed
   */
  def detectCodec(data: Array[Byte]): Option[CompressionCodec] =
    if (isCompressed(data) && data.length >= 2) {
      codecs.get(data(1))
    } else {
      None
    }

  /**
   * Create a compressing output stream that writes the proper header (magic byte + codec byte) followed by compressed
   * data.
   *
   * The returned stream automatically writes: 1. Magic byte (0x01) indicating compressed format 2. Codec byte
   * identifying the compression algorithm 3. Compressed data
   *
   * @param output
   *   The underlying output stream to write to
   * @param codec
   *   Optional compression codec. If None, returns the original stream unchanged (no compression)
   * @return
   *   An OutputStream that handles compression. Caller must close this stream when done.
   */
  def createCompressingOutputStream(output: OutputStream, codec: Option[CompressionCodec]): OutputStream =
    codec match {
      case Some(c) =>
        // Write header: magic byte + codec byte
        output.write(MAGIC_BYTE)
        output.write(c.codecByte)
        // Return compressing stream wrapper
        c.createCompressingOutputStream(output)

      case None =>
        // No compression - return original stream
        output
    }

  /**
   * Create a decompressing input stream that reads the header and automatically decompresses data.
   *
   * Automatically detects compressed format by checking for magic byte. If uncompressed (legacy format), returns a
   * stream that reads uncompressed data.
   *
   * @param input
   *   The underlying input stream to read from
   * @return
   *   An InputStream that handles decompression. Caller must close this stream when done.
   * @throws IllegalArgumentException
   *   if file is empty or compressed format is invalid
   * @throws UnsupportedOperationException
   *   if compression codec is unknown
   */
  def createDecompressingInputStream(input: InputStream): InputStream = {
    // Use pushback stream so we can "unread" bytes if uncompressed
    val pushback = new PushbackInputStream(input, 2)

    val firstByte = pushback.read()
    if (firstByte == -1) {
      throw new IllegalArgumentException("Transaction file is empty")
    }

    if (firstByte.toByte == MAGIC_BYTE) {
      // Compressed format - read codec byte
      val codecByte = pushback.read()
      if (codecByte == -1) {
        throw new IllegalArgumentException("Compressed file too short (missing codec byte)")
      }

      val codec = codecs.getOrElse(
        codecByte.toByte,
        throw new UnsupportedOperationException(
          s"Unknown compression codec: 0x${Integer.toHexString(codecByte & 0xff)}. " +
            s"Supported codecs: ${codecs.keys.map(b => s"0x${Integer.toHexString(b & 0xff)}").mkString(", ")}"
        )
      )

      // Return decompressing stream wrapper
      codec.createDecompressingInputStream(pushback)

    } else {
      // Legacy uncompressed format - push back the byte and return as-is
      pushback.unread(firstByte)
      pushback
    }
  }

  /**
   * Write the compression header (magic byte + codec byte) to an output stream.
   *
   * This is a utility method for cases where you need to write the header separately from creating the compressing
   * stream.
   *
   * @param output
   *   The output stream to write the header to
   * @param codec
   *   The compression codec (determines the codec byte)
   */
  def writeCompressionHeader(output: OutputStream, codec: CompressionCodec): Unit = {
    output.write(MAGIC_BYTE)
    output.write(codec.codecByte)
  }
}
