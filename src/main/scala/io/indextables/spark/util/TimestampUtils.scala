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

import java.sql.Timestamp
import java.time.LocalDateTime

/**
 * Utility functions for timestamp conversions.
 *
 * Spark internally stores timestamps as microseconds since epoch (Long).
 * This object provides consistent conversion functions to avoid duplication.
 */
object TimestampUtils {

  /**
   * Convert java.sql.Timestamp to microseconds since epoch.
   *
   * Spark's internal representation for timestamps is microseconds since epoch.
   * This function converts a java.sql.Timestamp (milliseconds + nanoseconds) to microseconds.
   *
   * @param ts java.sql.Timestamp to convert
   * @return microseconds since epoch as Long
   */
  def toMicros(ts: Timestamp): Long =
    ts.getTime * 1000L + (ts.getNanos / 1000L) % 1000L

  /**
   * Convert java.time.LocalDateTime to microseconds since epoch.
   *
   * Converts a LocalDateTime (assuming UTC timezone) to microseconds since epoch.
   *
   * @param ldt java.time.LocalDateTime to convert
   * @return microseconds since epoch as Long
   */
  def toMicros(ldt: LocalDateTime): Long = {
    val millis = ldt.atZone(java.time.ZoneOffset.UTC).toInstant.toEpochMilli
    (millis * 1000L) + (ldt.getNano / 1000L)
  }
}
