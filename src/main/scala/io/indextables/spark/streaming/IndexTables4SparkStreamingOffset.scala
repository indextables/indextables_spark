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

package io.indextables.spark.streaming

import org.apache.spark.sql.connector.read.streaming.Offset
import org.json4s._
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.{read, write}

/**
 * Streaming offset based on transaction log version.
 *
 * The offset encapsulates the transaction log version which serves as the
 * natural streaming offset for IndexTables4Spark tables. Versions are
 * monotonically increasing and immutable, making them perfect for tracking
 * streaming progress.
 *
 * @param version Transaction log version (exclusive lower bound for next batch).
 *                Version -1 means "from beginning" (includes version 0).
 * @param timestamp Optional timestamp when this version was committed (for monitoring/debugging)
 */
case class IndexTables4SparkStreamingOffset(
    version: Long,
    timestamp: Option[Long] = None
) extends Offset {

  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Serializes this offset to JSON for Spark's checkpoint mechanism.
   */
  override def json(): String = write(this)

  /**
   * Returns true if this offset is after the other offset.
   */
  def isAfter(other: IndexTables4SparkStreamingOffset): Boolean =
    this.version > other.version

  /**
   * Returns true if this offset is before the other offset.
   */
  def isBefore(other: IndexTables4SparkStreamingOffset): Boolean =
    this.version < other.version
}

object IndexTables4SparkStreamingOffset {
  implicit val formats: Formats = Serialization.formats(NoTypeHints)

  /**
   * Deserializes an offset from JSON.
   *
   * @param json JSON string from checkpoint
   * @return Deserialized offset
   */
  def fromJson(json: String): IndexTables4SparkStreamingOffset =
    read[IndexTables4SparkStreamingOffset](json)

  /**
   * Initial offset before any data.
   * Version -1 means "from beginning" - will include all data starting from version 0.
   */
  val INITIAL: IndexTables4SparkStreamingOffset =
    IndexTables4SparkStreamingOffset(version = -1L)
}
