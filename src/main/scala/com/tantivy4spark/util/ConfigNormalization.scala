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

package com.tantivy4spark.util

import org.apache.hadoop.conf.Configuration
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * Utility object for normalizing Tantivy4Spark configuration keys.
 *
 * This utility provides a centralized approach for handling configuration key normalization
 * between the legacy "spark.indextables.*" prefix and the new "spark.indextables.*" prefix.
 * All "spark.indextables.*" keys are normalized to "spark.indextables.*" internally
 * to maintain code cleanliness while supporting dual prefixes.
 */
object ConfigNormalization {

  /**
   * Normalizes a configuration key from spark.indextables.* to spark.indextables.*
   * if needed. Returns the key unchanged if it doesn't start with spark.indextables.
   */
  def normalizeKey(key: String): String = {
    if (key.startsWith("spark.indextables.")) {
      key.replace("spark.indextables.", "spark.indextables.")
    } else {
      key
    }
  }

  /**
   * Checks if a configuration key is a Tantivy4Spark-related key (either prefix).
   */
  def isTantivyKey(key: String): Boolean = {
    key.startsWith("spark.indextables.") || key.startsWith("spark.indextables.")
  }

  /**
   * Filters and normalizes a Map of configuration properties to extract only
   * Tantivy4Spark-related keys with normalized prefixes.
   */
  def filterAndNormalizeTantivyConfigs(configs: Map[String, String]): Map[String, String] = {
    configs.filter { case (key, _) =>
      isTantivyKey(key)
    }.map { case (key, value) =>
      normalizeKey(key) -> value
    }
  }

  /**
   * Filters and normalizes Tantivy4Spark configurations from Hadoop Configuration.
   */
  def extractTantivyConfigsFromHadoop(hadoopConf: Configuration): Map[String, String] = {
    val configs = mutable.Map[String, String]()
    val iter = hadoopConf.iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      if (isTantivyKey(entry.getKey) && entry.getValue != null) {
        val normalizedKey = normalizeKey(entry.getKey)
        configs.put(normalizedKey, entry.getValue)
      }
    }
    configs.toMap
  }

  /**
   * Filters and normalizes Tantivy4Spark configurations from SparkSession.
   */
  def extractTantivyConfigsFromSpark(spark: SparkSession): Map[String, String] = {
    try {
      spark.conf.getAll.filter { case (key, value) =>
        isTantivyKey(key) && value != null
      }.map { case (key, value) =>
        normalizeKey(key) -> value
      }.toMap
    } catch {
      case _: Exception => Map.empty[String, String]
    }
  }

  /**
   * Filters and normalizes Tantivy4Spark configurations from CaseInsensitiveStringMap.
   */
  def extractTantivyConfigsFromOptions(options: CaseInsensitiveStringMap): Map[String, String] = {
    options.asScala.filter { case (key, value) =>
      isTantivyKey(key) && value != null
    }.map { case (key, value) =>
      normalizeKey(key) -> value
    }.toMap
  }

  /**
   * Filters and normalizes Tantivy4Spark configurations from a regular Map.
   */
  def extractTantivyConfigsFromMap(configs: Map[String, String]): Map[String, String] = {
    configs.filter { case (key, value) =>
      isTantivyKey(key) && value != null
    }.map { case (key, value) =>
      normalizeKey(key) -> value
    }
  }

  /**
   * Copies normalized Tantivy4Spark configurations from a source Map to Hadoop Configuration.
   * This is useful for propagating configuration to executors.
   */
  def copyTantivyConfigsToHadoop(sourceConfigs: Map[String, String], hadoopConf: Configuration): Unit = {
    sourceConfigs.foreach { case (key, value) =>
      if (isTantivyKey(key)) {
        val normalizedKey = normalizeKey(key)
        hadoopConf.set(normalizedKey, value)
      }
    }
  }

  /**
   * Merges configuration maps with proper precedence handling and normalization.
   * Later maps in the sequence have higher precedence.
   *
   * @param configMaps Variable number of configuration maps, ordered by precedence (lowest to highest)
   * @return Merged and normalized configuration map
   */
  def mergeWithPrecedence(configMaps: Map[String, String]*): Map[String, String] = {
    val result = mutable.Map[String, String]()

    configMaps.foreach { configMap =>
      val normalized = extractTantivyConfigsFromMap(configMap)
      result ++= normalized
    }

    result.toMap
  }
}