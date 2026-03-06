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

package io.indextables.spark.arrow

/**
 * Configuration for Arrow FFI aggregation on the read path.
 *
 * When enabled, aggregation results are exported from tantivy4java as Arrow columnar data via the C Data Interface,
 * eliminating per-bucket JNI overhead for GROUP BY and bucket aggregations.
 *
 * @param enabled
 *   Whether Arrow FFI aggregation read path is enabled (default: true)
 */
case class AggregationArrowFfiConfig(enabled: Boolean = AggregationArrowFfiConfig.DEFAULT_ENABLED)

object AggregationArrowFfiConfig {
  val KEY_ENABLED     = "spark.indextables.read.aggregation.arrowFfi.enabled"
  // Enabled by default since tantivy4java 0.31.2 provides aggregateArrowFfi/multiSplitAggregateArrowFfi
  val DEFAULT_ENABLED = true

  def fromMap(config: Map[String, String]): AggregationArrowFfiConfig = {
    val lowerCaseConfig              = config.map { case (k, v) => k.toLowerCase -> v }
    def get(key: String): Option[String] = lowerCaseConfig.get(key.toLowerCase)

    AggregationArrowFfiConfig(
      enabled = get(KEY_ENABLED).map(_.toBoolean).getOrElse(DEFAULT_ENABLED)
    )
  }

  def isEnabled(config: Map[String, String]): Boolean =
    fromMap(config).enabled
}
