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
 * Canonical definitions for companion split indexing modes. All mode classification
 * logic should go through this object to prevent drift across the codebase.
 */
object IndexingModes {

  /** Modes that are recognized as exact string values (case-insensitive). */
  val recognizedModes: Set[String] = Set(
    "string", "text", "ip", "ipaddress", "json",
    "exact_only", "text_uuid_exactonly", "text_uuid_strip"
  )

  /** Mode prefixes that require a regex suffix after the colon. */
  val regexPrefixes: Seq[String] = Seq(
    "text_custom_exactonly:", "text_custom_strip:"
  )

  /** Human-readable list of valid modes for error messages. */
  val validModesDescription: String =
    "string, text, ip, ipaddress, json, exact_only, " +
      "text_uuid_exactonly, text_uuid_strip, text_custom_exactonly:<regex>, text_custom_strip:<regex>"

  /** Check if a mode value is recognized (case-insensitive). */
  def isRecognized(mode: String): Boolean = {
    val lower = mode.toLowerCase
    recognizedModes.contains(lower) || regexPrefixes.exists(p => lower.startsWith(p))
  }

  /** Check if a mode is a compact string mode (exact_only or text_*). */
  def isCompactStringMode(mode: String): Boolean = {
    val lower = mode.toLowerCase
    lower == "exact_only" ||
    lower == "text_uuid_exactonly" ||
    lower == "text_uuid_strip" ||
    lower.startsWith("text_custom_exactonly:") ||
    lower.startsWith("text_custom_strip:")
  }

  /** Check if a mode supports EqualTo filter pushdown to tantivy. */
  def supportsExactMatchPushdown(mode: String): Boolean = {
    val lower = mode.toLowerCase
    lower match {
      case "text"                             => false
      case "exact_only"                       => true
      case m if m.startsWith("text_")         => false
      case _                                  => true // string, ip, ipaddress, json, etc.
    }
  }

  /** Extract the regex portion from a custom mode, or None if not a custom mode. */
  def extractCustomRegex(mode: String): Option[String] = {
    val lower = mode.toLowerCase
    regexPrefixes.collectFirst {
      case prefix if lower.startsWith(prefix) => mode.substring(prefix.length)
    }
  }

  /** Normalize a mode value for the native tantivy4java layer.
    *
    * Lowercases the mode prefix (e.g., "EXACT_ONLY" → "exact_only") while
    * preserving the case of any regex suffix (e.g., "TEXT_CUSTOM_STRIP:ORD-\d{8}"
    * → "text_custom_strip:ORD-\d{8}"). This is necessary because:
    * - The Rust parser (`parse_tokenizer_override`) matches mode prefixes case-sensitively
    * - Regex patterns may be case-sensitive (e.g., "ORD" ≠ "ord")
    */
  def normalizeForNative(mode: String): String = {
    val colonIdx = mode.indexOf(':')
    if (colonIdx >= 0) {
      mode.substring(0, colonIdx).toLowerCase + mode.substring(colonIdx)
    } else {
      mode.toLowerCase
    }
  }
}
