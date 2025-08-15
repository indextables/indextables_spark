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

package com.tantivy4spark.bloom

import org.slf4j.LoggerFactory
import com.google.common.hash.{BloomFilter => GuavaBloomFilter, Funnels}
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.charset.StandardCharsets
import scala.collection.mutable

/**
 * Bloom filter wrapper around Google Guava's well-tested implementation.
 * 
 * Design considerations:
 * - Uses proven Guava BloomFilter implementation
 * - False positive rate: Configurable (default ~1%)
 * - Serialization: Uses Guava's built-in serialization
 * - Thread-safe and optimized hash functions
 */
class BloomFilter(
    val expectedItems: Int,
    val falsePositiveRate: Double = 0.01
) {
  
  private val logger = LoggerFactory.getLogger(classOf[BloomFilter])
  
  // Use Guava's BloomFilter with String funnel
  private var guavaFilter: GuavaBloomFilter[String] = GuavaBloomFilter.create[String](
    Funnels.stringFunnel(StandardCharsets.UTF_8), 
    expectedItems, 
    falsePositiveRate
  )
  
  private var itemCount = 0
  
  // Method to set item count for deserialization  
  private[bloom] def setItemCount(count: Int): Unit = {
    itemCount = count
  }
  
  // Method to set the internal filter for deserialization
  private[bloom] def setGuavaFilter(filter: GuavaBloomFilter[String]): Unit = {
    guavaFilter = filter
  }
  
  /**
   * Add a token to the bloom filter
   */
  def add(token: String): Unit = {
    guavaFilter.put(token)
    itemCount += 1
  }
  
  /**
   * Test if a token might be present in the bloom filter
   * Returns false if definitely not present, true if might be present
   */
  def mightContain(token: String): Boolean = {
    guavaFilter.mightContain(token)
  }
  
  /**
   * Get current estimated false positive rate based on actual usage
   */
  def getCurrentFalsePositiveRate: Double = {
    guavaFilter.expectedFpp()
  }
  
  /**
   * Serialize bloom filter to compact byte array for storage
   */
  def serialize(): Array[Byte] = {
    val baos = new ByteArrayOutputStream()
    guavaFilter.writeTo(baos)
    val filterBytes = baos.toByteArray
    
    // Prepend metadata: itemCount, falsePositiveRate, filterBytesLength
    val metadataSize = 4 + 8 + 4 // int + double + int
    val buffer = java.nio.ByteBuffer.allocate(metadataSize + filterBytes.length)
    buffer.putInt(itemCount)
    buffer.putDouble(falsePositiveRate)
    buffer.putInt(filterBytes.length)
    buffer.put(filterBytes)
    buffer.array()
  }
  
  def getStats: Map[String, Any] = {
    Map(
      "expectedItems" -> expectedItems,
      "itemCount" -> itemCount,
      "targetFalsePositiveRate" -> falsePositiveRate,
      "currentFalsePositiveRate" -> getCurrentFalsePositiveRate,
      "approximateElementCount" -> guavaFilter.approximateElementCount()
    )
  }
}

object BloomFilter {
  private val logger = LoggerFactory.getLogger(BloomFilter.getClass)
  
  /**
   * Deserialize bloom filter from byte array
   */
  def deserialize(data: Array[Byte]): BloomFilter = {
    try {
      val buffer = java.nio.ByteBuffer.wrap(data)
      val itemCount = buffer.getInt()
      val falsePositiveRate = buffer.getDouble()
      val filterBytesLength = buffer.getInt()
      
      val filterBytes = new Array[Byte](filterBytesLength)
      buffer.get(filterBytes)
      
      // Deserialize Guava bloom filter
      val bais = new ByteArrayInputStream(filterBytes)
      val guavaFilter = GuavaBloomFilter.readFrom[String](bais, Funnels.stringFunnel(StandardCharsets.UTF_8))
      
      // Create wrapper with original parameters
      val filter = new BloomFilter(itemCount, falsePositiveRate)
      filter.setGuavaFilter(guavaFilter)
      filter.setItemCount(itemCount)
      
      logger.info(s"Deserialized Guava bloom filter: items=$itemCount, fpr=$falsePositiveRate")
      filter
    } catch {
      case e: Exception =>
        logger.error(s"Failed to deserialize bloom filter from ${data.length} bytes", e)
        // Return a minimal working filter as fallback
        new BloomFilter(100, 0.01)
    }
  }
  
  /**
   * Create bloom filter from collection of tokens
   */
  def fromTokens(tokens: Iterable[String], falsePositiveRate: Double = 0.01): BloomFilter = {
    val filter = new BloomFilter(tokens.size, falsePositiveRate)
    tokens.foreach(filter.add)
    filter
  }
}

/**
 * Manages bloom filters for text columns with intelligent tokenization
 * and S3-optimized storage patterns.
 */
class BloomFilterManager {
  private val logger = LoggerFactory.getLogger(classOf[BloomFilterManager])
  private val textTokenizer = new TextTokenizer()
  
  /**
   * Create bloom filters for text columns in a dataset
   * Returns Map[columnName -> BloomFilter]
   */
  def createBloomFilters(
      textColumns: Map[String, Iterable[String]], 
      falsePositiveRate: Double = 0.01
  ): Map[String, BloomFilter] = {
    
    textColumns.map { case (columnName, values) =>
      logger.info(s"Creating bloom filter for column '$columnName' with ${values.size} values")
      
      // Tokenize all text values for this column
      val allTokens = values.flatMap { text =>
        if (text != null && text.nonEmpty) {
          textTokenizer.tokenize(text)
        } else {
          Seq.empty
        }
      }
      
      val uniqueTokens = allTokens.toSet
      logger.info(s"Column '$columnName': ${values.size} values -> ${allTokens.size} tokens -> ${uniqueTokens.size} unique tokens")
      
      val filter = BloomFilter.fromTokens(uniqueTokens, falsePositiveRate)
      logger.info(s"Created bloom filter for '$columnName': ${filter.getStats}")
      
      columnName -> filter
    }.toMap
  }
  
  /**
   * Test if any of the search terms might be present in the given bloom filters
   * Uses smart query expansion and tokenization
   */
  def mightContainAnyTerm(
      bloomFilters: Map[String, BloomFilter],
      searchTerms: Iterable[String]
  ): Boolean = {
    
    if (bloomFilters.isEmpty || searchTerms.isEmpty) {
      return true // Conservative approach - include file if no filters or terms
    }
    
    // For each search term, check if it might be fully present
    searchTerms.exists { term =>
      mightContainCompleteTerm(bloomFilters, term)
    }
  }
  
  /**
   * Test if a complete term might be present by checking all its n-grams
   */
  private def mightContainCompleteTerm(
      bloomFilters: Map[String, BloomFilter],
      term: String
  ): Boolean = {
    if (term == null || term.isEmpty) return true
    
    val normalizedTerm = term.toLowerCase.trim
    if (normalizedTerm.length < textTokenizer.minTokenLength) return true
    
    // Get all n-grams for this specific term
    val termNgrams = generateNgramsForTerm(normalizedTerm)
    
    if (termNgrams.isEmpty) {
      // If no n-grams, fall back to exact term matching
      val exactTerms = Set(normalizedTerm)
      return bloomFilters.values.exists { filter =>
        exactTerms.exists(filter.mightContain)
      }
    }
    
    // For the term to be present, ALL its n-grams must be present in at least one filter
    bloomFilters.values.exists { filter =>
      termNgrams.forall(filter.mightContain)
    }
  }
  
  /**
   * Generate n-grams specifically for a single term (more precise than general tokenization)
   */
  private def generateNgramsForTerm(term: String): Set[String] = {
    val ngramSize = textTokenizer.ngramSize
    if (term.length < ngramSize) {
      return Set(term)
    }
    
    (0 to term.length - ngramSize).map { i =>
      term.substring(i, i + ngramSize)
    }.toSet
  }
  
  /**
   * Advanced search with column-specific filtering
   */
  def evaluateColumnSearch(
      bloomFilters: Map[String, BloomFilter],
      columnSearches: Map[String, Iterable[String]]
  ): Boolean = {
    
    if (bloomFilters.isEmpty || columnSearches.isEmpty) {
      return true
    }
    
    // For each column search, check if the bloom filter might contain the terms
    columnSearches.forall { case (columnName, searchTerms) =>
      bloomFilters.get(columnName) match {
        case Some(filter) =>
          val searchTokens = searchTerms.flatMap(textTokenizer.tokenize)
          searchTokens.exists(filter.mightContain)
        case None =>
          true // No bloom filter for this column - include conservatively
      }
    }
  }
  
  /**
   * Get detailed statistics for all bloom filters
   */
  def getDetailedStats(bloomFilters: Map[String, BloomFilter]): Map[String, Map[String, Any]] = {
    bloomFilters.map { case (column, filter) =>
      column -> filter.getStats
    }
  }
}

/**
 * Splunk-style text tokenizer that handles various text formats intelligently
 */
class TextTokenizer {
  private val logger = LoggerFactory.getLogger(classOf[TextTokenizer])
  
  // Configuration for tokenization
  val minTokenLength = 2 // Made public for access from BloomFilterManager
  private val maxTokenLength = 50
  private val includeNgrams = true
  val ngramSize = 3 // Made public for access from BloomFilterManager
  
  /**
   * Tokenize text into searchable terms using Splunk-style approach:
   * - Word boundaries (alphanumeric sequences)
   * - Common separators (dots, underscores, hyphens)
   * - N-grams for partial matching
   * - Case insensitive
   * - Unicode support
   */
  def tokenize(text: String): Set[String] = {
    if (text == null || text.isEmpty) {
      return Set.empty
    }
    
    val normalized = text.toLowerCase.trim
    val tokens = mutable.Set[String]()
    
    // 1. Extract word sequences (including Unicode letters and digits)
    val wordPattern = "[\\p{L}\\p{N}]+".r // Unicode letters and numbers
    val words = wordPattern.findAllIn(normalized).toSeq
    tokens ++= words.filter(w => w.length >= minTokenLength && w.length <= maxTokenLength)
    
    // 2. Extract terms separated by common delimiters
    val delimiterTokens = normalized.split("[^\\p{L}\\p{N}]+").filter(_.nonEmpty)
    tokens ++= delimiterTokens.filter(t => t.length >= minTokenLength && t.length <= maxTokenLength)
    
    // 3. Create n-grams for partial matching (like Splunk's substring search)
    if (includeNgrams && normalized.length >= ngramSize) {
      val ngrams = for {
        i <- 0 to normalized.length - ngramSize
        ngram = normalized.substring(i, i + ngramSize)
        if ngram.matches("[\\p{L}\\p{N}]+") // Only letter/number n-grams
      } yield ngram
      
      tokens ++= ngrams
    }
    
    // 4. Add the full normalized text as a token (for exact phrase matching)
    if (normalized.length >= minTokenLength && normalized.length <= maxTokenLength) {
      tokens += normalized
    }
    
    val result = tokens.toSet
    logger.debug(s"Tokenized '$text' -> ${result.size} tokens: ${result.take(10)}")
    result
  }
  
  /**
   * Extract searchable terms from a query string
   * Handles quoted phrases, boolean operators, wildcards
   */
  def extractSearchTerms(query: String): Set[String] = {
    if (query == null || query.isEmpty) {
      return Set.empty
    }
    
    val terms = mutable.Set[String]()
    
    // Handle quoted phrases
    val quotedPattern = "\"([^\"]+)\"".r
    val quotes = quotedPattern.findAllMatchIn(query).map(_.group(1)).toSeq
    terms ++= quotes.flatMap(tokenize)
    
    // Remove quoted parts and process the rest
    val withoutQuotes = quotedPattern.replaceAllIn(query, " ")
    
    // Extract individual terms (ignore common boolean operators)
    val booleanOperators = Set("and", "or", "not", "AND", "OR", "NOT")
    val termPattern = "[\\p{L}\\p{N}]+".r
    val individualTerms = termPattern.findAllIn(withoutQuotes)
      .filter(t => !booleanOperators.contains(t) && t.length >= minTokenLength)
      .toSeq
    
    terms ++= individualTerms.flatMap(tokenize)
    
    terms.toSet
  }
}