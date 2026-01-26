package io.indextables.spark.util

import org.apache.hadoop.conf.Configuration

import org.slf4j.LoggerFactory

/** Configuration source abstraction for priority-based resolution. */
sealed trait ConfigSource {
  def get(key: String): Option[String]
  def name: String
}

case class OptionsConfigSource(options: java.util.Map[String, String]) extends ConfigSource {
  override def get(key: String): Option[String] =
    // Try original key first, then lowercase version (for CaseInsensitiveStringMap compatibility)
    Option(options.get(key)).orElse(Option(options.get(key.toLowerCase)))
  override def name: String = "DataFrame options"
}

case class HadoopConfigSource(hadoopConf: Configuration, prefix: String = "") extends ConfigSource {
  override def get(key: String): Option[String] = {
    val fullKey = if (prefix.isEmpty) key else s"$prefix.$key"
    // Try original key first, then lowercase version (for CaseInsensitiveStringMap compatibility)
    Option(hadoopConf.get(fullKey)).orElse(Option(hadoopConf.get(fullKey.toLowerCase)))
  }
  override def name: String = if (prefix.isEmpty) "Hadoop config" else s"Hadoop config ($prefix)"
}

case class SparkConfigSource(sparkConf: org.apache.spark.SparkConf) extends ConfigSource {
  override def get(key: String): Option[String] = sparkConf.getOption(key)
  override def name: String                     = "Spark config"
}

case class EnvironmentConfigSource() extends ConfigSource {
  override def get(key: String): Option[String] = sys.env.get(key)
  override def name: String                     = "Environment variables"
}

/**
 * ConfigSource that reads from a Map[String, String] directly.
 *
 * This is the fast path for credential resolution - it avoids creating Hadoop Configuration
 * objects when we already have the config as a Map. This is critical for performance when
 * resolving credentials on executors, as Hadoop Configuration creation is expensive.
 *
 * @param config Map containing configuration key-value pairs
 * @param prefix Optional prefix to prepend to keys (e.g., "spark.indextables.databricks")
 */
case class MapConfigSource(config: Map[String, String], prefix: String = "") extends ConfigSource {
  override def get(key: String): Option[String] = {
    val fullKey = if (prefix.isEmpty) key else s"$prefix.$key"
    // Try both original key and lowercase version (for CaseInsensitiveStringMap compatibility)
    config.get(fullKey).orElse(config.get(fullKey.toLowerCase))
  }
  override def name: String = if (prefix.isEmpty) "Map config" else s"Map config ($prefix)"
}

/**
 * Utility for resolving configuration values from multiple sources with priority ordering.
 *
 * Provides consistent credential resolution across AWS, Azure, and other configuration needs.
 */
object ConfigurationResolver {

  private val logger = LoggerFactory.getLogger(getClass)

  /**
   * Resolves a string configuration value from multiple sources in priority order.
   *
   * @param key
   *   Configuration key to resolve
   * @param sources
   *   Ordered sequence of configuration sources (first match wins)
   * @param logMask
   *   If true, masks the value in logs (for credentials)
   * @return
   *   Resolved value, or None if not found in any source
   */
  def resolveString(
    key: String,
    sources: Seq[ConfigSource],
    logMask: Boolean = false
  ): Option[String] = {
    sources.foreach { source =>
      source.get(key) match {
        case Some(value) if value.nonEmpty =>
          val displayValue = if (logMask) "****" else value
          logger.debug(s"Resolved '$key' from ${source.name}: $displayValue")
          return Some(value)
        case _ =>
      }
    }
    logger.debug(s"Could not resolve '$key' from any source")
    None
  }

  /**
   * Resolves a boolean configuration value from multiple sources.
   *
   * @param key
   *   Configuration key to resolve
   * @param sources
   *   Ordered sequence of configuration sources
   * @param default
   *   Default value if not found
   * @return
   *   Resolved boolean value
   */
  def resolveBoolean(
    key: String,
    sources: Seq[ConfigSource],
    default: Boolean = false
  ): Boolean =
    resolveString(key, sources, logMask = false) match {
      case Some(value) =>
        val boolValue = value.toLowerCase match {
          case "true" | "1" | "yes" | "on"  => true
          case "false" | "0" | "no" | "off" => false
          case _ =>
            logger.warn(s"Invalid boolean value for '$key': '$value', using default: $default")
            default
        }
        logger.debug(s"Resolved boolean '$key': $boolValue")
        boolValue
      case None =>
        logger.debug(s"Using default for '$key': $default")
        default
    }

  /**
   * Resolves a long configuration value from multiple sources.
   *
   * @param key
   *   Configuration key to resolve
   * @param sources
   *   Ordered sequence of configuration sources
   * @param default
   *   Default value if not found
   * @return
   *   Resolved long value
   */
  def resolveLong(
    key: String,
    sources: Seq[ConfigSource],
    default: Long
  ): Long =
    resolveString(key, sources, logMask = false) match {
      case Some(value) =>
        try {
          val longValue = value.toLong
          logger.debug(s"Resolved long '$key': $longValue")
          longValue
        } catch {
          case e: NumberFormatException =>
            logger.warn(s"Invalid long value for '$key': '$value', using default: $default", e)
            default
        }
      case None =>
        logger.debug(s"Using default for '$key': $default")
        default
    }

  /**
   * Resolves an integer configuration value from multiple sources.
   *
   * @param key
   *   Configuration key to resolve
   * @param sources
   *   Ordered sequence of configuration sources
   * @param default
   *   Default value if not found
   * @return
   *   Resolved integer value
   */
  def resolveInt(
    key: String,
    sources: Seq[ConfigSource],
    default: Int
  ): Int =
    resolveString(key, sources, logMask = false) match {
      case Some(value) =>
        try {
          val intValue = value.toInt
          logger.debug(s"Resolved int '$key': $intValue")
          intValue
        } catch {
          case e: NumberFormatException =>
            logger.warn(s"Invalid int value for '$key': '$value', using default: $default", e)
            default
        }
      case None =>
        logger.debug(s"Using default for '$key': $default")
        default
    }

  /**
   * Resolves multiple configuration keys as a batch.
   *
   * @param keys
   *   Configuration keys to resolve
   * @param sources
   *   Ordered sequence of configuration sources
   * @param logMask
   *   If true, masks values in logs
   * @return
   *   Map of resolved key-value pairs (only includes found keys)
   */
  def resolveBatch(
    keys: Seq[String],
    sources: Seq[ConfigSource],
    logMask: Boolean = false
  ): Map[String, String] =
    keys.flatMap(key => resolveString(key, sources, logMask).map(key -> _)).toMap

  /**
   * Creates standard AWS credential resolution sources in priority order.
   *
   * Priority: DataFrame options > Hadoop tantivy > Hadoop S3a > Environment
   */
  def createAWSCredentialSources(
    options: java.util.Map[String, String],
    hadoopConf: Configuration
  ): Seq[ConfigSource] = Seq(
    OptionsConfigSource(options),
    HadoopConfigSource(hadoopConf, "spark.indextables.aws"),
    HadoopConfigSource(hadoopConf, "spark.hadoop.fs.s3a"),
    HadoopConfigSource(hadoopConf, "fs.s3a"),
    EnvironmentConfigSource()
  )

  /**
   * Creates standard Azure credential resolution sources in priority order.
   *
   * Priority: DataFrame options > Hadoop indextables > Hadoop config > Environment
   */
  def createAzureCredentialSources(
    options: java.util.Map[String, String],
    hadoopConf: Configuration
  ): Seq[ConfigSource] = Seq(
    OptionsConfigSource(options),
    HadoopConfigSource(hadoopConf, "spark.indextables.azure"),
    HadoopConfigSource(hadoopConf),
    EnvironmentConfigSource()
  )

  /**
   * Creates standard general configuration resolution sources in priority order.
   *
   * Priority: DataFrame options > Hadoop indextables config > Hadoop config
   */
  def createGeneralConfigSources(
    options: java.util.Map[String, String],
    hadoopConf: Configuration
  ): Seq[ConfigSource] = Seq(
    OptionsConfigSource(options),
    HadoopConfigSource(hadoopConf, "spark.indextables"),
    HadoopConfigSource(hadoopConf)
  )
}
