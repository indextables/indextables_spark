package io.indextables.spark.transaction.compression

/**
 * Utilities for reading/writing compressed transaction log files.
 *
 * Provides automatic compression detection and transparent compression/decompression
 * for transaction log and checkpoint files.
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
   * @param data Raw file bytes
   * @return Decompressed JSON bytes
   * @throws IllegalArgumentException if file is empty or compressed format is invalid
   * @throws UnsupportedOperationException if compression codec is unknown
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
        codecByte, {
          throw new UnsupportedOperationException(
            s"Unknown compression codec: 0x${Integer.toHexString(codecByte & 0xFF)}. " +
              s"Supported codecs: ${codecs.keys.map(b => s"0x${Integer.toHexString(b & 0xFF)}").mkString(", ")}"
          )
        }
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
   * @param jsonData JSON bytes to write
   * @param codec Optional compression codec (None = uncompressed)
   * @return File bytes with compression headers if applicable
   */
  def writeTransactionFile(jsonData: Array[Byte], codec: Option[CompressionCodec]): Array[Byte] = {
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
  }

  /**
   * Get codec from configuration.
   *
   * @param compressionEnabled Whether compression is enabled
   * @param codecName Codec name ("gzip", "none")
   * @param gzipLevel GZIP compression level (0-9)
   * @return Optional compression codec
   * @throws IllegalArgumentException if codec name is unknown
   */
  def getCodec(
      compressionEnabled: Boolean,
      codecName: String,
      gzipLevel: Int = 6
  ): Option[CompressionCodec] = {
    if (!compressionEnabled || codecName.equalsIgnoreCase("none")) {
      None
    } else if (codecName.equalsIgnoreCase("gzip")) {
      Some(new GzipCompressionCodec(gzipLevel))
    } else {
      throw new IllegalArgumentException(
        s"Unknown compression codec: $codecName (supported: gzip, none)"
      )
    }
  }

  /**
   * Check if file data is compressed.
   *
   * @param data File bytes
   * @return true if file is compressed (starts with magic byte)
   */
  def isCompressed(data: Array[Byte]): Boolean = {
    data.nonEmpty && data(0) == MAGIC_BYTE
  }

  /**
   * Get compression codec from file data.
   *
   * @param data File bytes
   * @return Optional compression codec if file is compressed
   */
  def detectCodec(data: Array[Byte]): Option[CompressionCodec] = {
    if (isCompressed(data) && data.length >= 2) {
      codecs.get(data(1))
    } else {
      None
    }
  }
}
