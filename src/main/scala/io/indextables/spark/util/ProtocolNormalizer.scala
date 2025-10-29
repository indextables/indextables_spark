package io.indextables.spark.util

import org.slf4j.LoggerFactory

/**
 * Utility for normalizing cloud storage protocol schemes to tantivy4java-compatible formats.
 *
 * Supported normalizations:
 *   - S3: s3a://, s3n:// → s3://
 *   - Azure: wasb://, wasbs://, abfs://, abfss:// → azure://
 */
object ProtocolNormalizer {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Normalizes S3 protocol schemes to s3:// format.
   *
   * @param path
   *   The path to normalize
   * @return
   *   Normalized path with s3:// scheme, or original path if not S3
   */
  def normalizeS3Protocol(path: String): String =
    if (path.startsWith("s3a://") || path.startsWith("s3n://")) {
      val normalized = path.replaceFirst("^s3[an]://", "s3://")
      logger.debug(s"Normalized S3 protocol: $path → $normalized")
      normalized
    } else {
      path
    }

  /**
   * Normalizes Azure protocol schemes to azure:// format.
   *
   * Handles: wasb://, wasbs://, abfs://, abfss:// Extracts container name and path from full Azure URLs
   *
   * @param path
   *   The path to normalize
   * @return
   *   Normalized path with azure:// scheme, or original path if not Azure
   */
  def normalizeAzureProtocol(path: String): String =
    if (path.startsWith("wasb://") || path.startsWith("wasbs://")) {
      // Extract: wasb[s]://container@account.blob.core.windows.net/path → azure://container/path
      val pattern = """wasbs?://([^@]+)@[^/]+/(.*)""".r
      path match {
        case pattern(container, pathPart) =>
          val normalized = s"azure://$container/$pathPart"
          logger.debug(s"Normalized Azure WASB protocol: $path → $normalized")
          normalized
        case _ =>
          logger.warn(s"Could not parse Azure WASB URL: $path")
          path
      }
    } else if (path.startsWith("abfs://") || path.startsWith("abfss://")) {
      // Extract: abfs[s]://container@account.dfs.core.windows.net/path → azure://container/path
      val pattern = """abfss?://([^@]+)@[^/]+/(.*)""".r
      path match {
        case pattern(container, pathPart) =>
          val normalized = s"azure://$container/$pathPart"
          logger.debug(s"Normalized Azure ABFS protocol: $path → $normalized")
          normalized
        case _ =>
          logger.warn(s"Could not parse Azure ABFS URL: $path")
          path
      }
    } else {
      path
    }

  /**
   * Normalizes all supported cloud storage protocols.
   *
   * @param path
   *   The path to normalize
   * @return
   *   Normalized path, or original if no normalization needed
   */
  def normalizeAllProtocols(path: String): String = {
    val afterS3    = normalizeS3Protocol(path)
    val afterAzure = normalizeAzureProtocol(afterS3)
    afterAzure
  }

  /**
   * Checks if a path uses an S3 protocol scheme.
   *
   * @param path
   *   The path to check
   * @return
   *   true if path starts with s3://, s3a://, or s3n://
   */
  def isS3Path(path: String): Boolean =
    path.startsWith("s3://") || path.startsWith("s3a://") || path.startsWith("s3n://")

  /**
   * Checks if a path uses an Azure protocol scheme.
   *
   * @param path
   *   The path to check
   * @return
   *   true if path starts with azure://, wasb://, wasbs://, abfs://, or abfss://
   */
  def isAzurePath(path: String): Boolean =
    path.startsWith("azure://") ||
      path.startsWith("wasb://") ||
      path.startsWith("wasbs://") ||
      path.startsWith("abfs://") ||
      path.startsWith("abfss://")
}
