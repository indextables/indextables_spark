/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.indextables.spark.util

/**
 * Utility object for redacting sensitive configuration values.
 *
 * Provides consistent credential redaction logic used across logging and debugging operations.
 */
object CredentialRedaction {

  /** Patterns that indicate a sensitive configuration key (case-insensitive). */
  private val sensitivePatterns = Set(
    "secret",
    "key",
    "password",
    "token",
    "credential",
    "session"
  )

  /** The redacted value placeholder. */
  val REDACTED = "***REDACTED***"

  /**
   * Check if a configuration key contains sensitive information.
   *
   * @param key
   *   The configuration key to check
   * @return
   *   true if the key appears to contain sensitive information
   */
  def isSensitiveKey(key: String): Boolean = {
    val lowerKey = key.toLowerCase
    sensitivePatterns.exists(pattern => lowerKey.contains(pattern))
  }

  /**
   * Redact a configuration value if its key indicates sensitive information.
   *
   * @param key
   *   The configuration key
   * @param value
   *   The configuration value
   * @return
   *   The original value if not sensitive, or REDACTED if sensitive
   */
  def redactValue(key: String, value: String): String =
    if (isSensitiveKey(key)) REDACTED else value

  /**
   * Redact sensitive values in a configuration map.
   *
   * @param configs
   *   The configuration map to redact
   * @return
   *   A new map with sensitive values redacted
   */
  def redactConfigMap(configs: Map[String, String]): Map[String, String] =
    configs.map { case (k, v) => k -> redactValue(k, v) }

  /**
   * Redact sensitive values in a Java configuration map.
   *
   * @param configs
   *   The Java configuration map to redact
   * @return
   *   A new Scala map with sensitive values redacted
   */
  def redactJavaConfigMap(configs: java.util.Map[String, String]): Map[String, String] = {
    import scala.jdk.CollectionConverters._
    configs.asScala.map { case (k, v) => k -> redactValue(k, v) }.toMap
  }

  /**
   * Format a configuration map for logging with sensitive values redacted.
   *
   * @param configs
   *   The configuration map to format
   * @param prefix
   *   Optional prefix for the log message
   * @return
   *   A string representation of the redacted configuration
   */
  def formatForLogging(configs: Map[String, String], prefix: String = ""): String = {
    val redacted = redactConfigMap(configs)
    val entries  = redacted.map { case (k, v) => s"  $k = $v" }.mkString("\n")
    if (prefix.nonEmpty) s"$prefix\n$entries" else entries
  }
}
