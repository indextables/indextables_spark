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

package io.indextables.spark.transaction

import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.cache.{Cache, CacheBuilder}
import com.google.common.cache.{CacheStats => GuavaCacheStats}
import org.slf4j.LoggerFactory

/**
 * Global caches for immutable metadata shared across transaction log instances.
 *
 * After the migration to NativeTransactionLog, this object retains only:
 *   - DocMappingMetadata cache (used by scan builders and readers to avoid repeated JSON parsing)
 *   - Global JSON parse counter (used for testing/debugging)
 *   - Filtered schema cache (used by scan builders for schema projection)
 */
object EnhancedTransactionLogCache {
  private val logger = LoggerFactory.getLogger(getClass)

  // ============================================================================
  // GLOBAL JSON PARSE COUNTER
  // ============================================================================
  private val globalJsonParseCounter = new java.util.concurrent.atomic.AtomicLong(0)

  @volatile private var throwOnJsonParse: Boolean = false

  def getGlobalJsonParseCount(): Long = globalJsonParseCounter.get()

  def resetGlobalJsonParseCounter(): Unit = globalJsonParseCounter.set(0)

  def enableThrowOnJsonParse(): Unit = throwOnJsonParse = true

  def disableThrowOnJsonParse(): Unit = throwOnJsonParse = false

  def incrementGlobalJsonParseCounter(): Unit = {
    globalJsonParseCounter.incrementAndGet()
    if (throwOnJsonParse) {
      throw new RuntimeException("DEBUG: JSON parse detected - stack trace for debugging")
    }
  }

  // ============================================================================
  // DOC MAPPING METADATA CACHE
  // ============================================================================
  private lazy val globalDocMappingMetadataCache: Cache[String, DocMappingMetadata] =
    CacheBuilder
      .newBuilder()
      .maximumSize(500)
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .recordStats()
      .build[String, DocMappingMetadata]()

  // ============================================================================
  // FILTERED SCHEMA CACHE
  // ============================================================================
  private lazy val globalFilteredSchemaCache: Cache[String, String] =
    CacheBuilder
      .newBuilder()
      .maximumSize(200)
      .expireAfterAccess(30, TimeUnit.MINUTES)
      .recordStats()
      .build[String, String]()

  private def atomicGetOrCompute[K, V](
    cache: Cache[K, V],
    key: K,
    compute: => V
  ): V =
    try
      cache.get(key, new Callable[V] { override def call(): V = compute })
    catch {
      case e: java.util.concurrent.ExecutionException => throw e.getCause
    }

  def getOrComputeDocMappingMetadata(key: String, docMappingJson: String): DocMappingMetadata =
    atomicGetOrCompute(globalDocMappingMetadataCache, key, DocMappingMetadata.parse(docMappingJson))

  def getDocMappingMetadata(addAction: AddAction): DocMappingMetadata =
    addAction.docMappingJson match {
      case Some(json) =>
        val key = addAction.docMappingRef.getOrElse(json)
        getOrComputeDocMappingMetadata(key, json)
      case None =>
        DocMappingMetadata.empty
    }

  def getOrComputeFilteredSchema(schemaHash: String, compute: => String): String =
    atomicGetOrCompute(globalFilteredSchemaCache, schemaHash, compute)

  /**
   * Get global cache statistics. Returns a tuple of GuavaCacheStats for compatibility with existing callers.
   *
   * Order: (docMappingCache, filteredSchemaCache, empty, empty, empty, empty) — remaining slots are empty stats for
   * backward compatibility with callers that destructure the 6-tuple.
   */
  def getGlobalCacheStats()
    : (GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats, GuavaCacheStats) = {
    val empty = new GuavaCacheStats(0, 0, 0, 0, 0, 0)
    (
      globalDocMappingMetadataCache.stats(),
      globalFilteredSchemaCache.stats(),
      empty,
      empty,
      empty,
      empty
    )
  }

  def clearGlobalCaches(): Unit = {
    globalDocMappingMetadataCache.invalidateAll()
    globalFilteredSchemaCache.invalidateAll()
    logger.info("Cleared global caches")
  }
}
