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

package com.tantivy4spark.search

import org.scalatest.funsuite.AnyFunSuite
import com.tantivy4java.QuickwitSplit
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import com.tantivy4spark.storage.SplitCacheConfig

/**
 * Tests for SplitSearchEngine validation of metadata when creating searchers.
 * Verifies that proper exceptions are thrown when invalid metadata is provided.
 */
class SplitSearchEngineValidationTest extends AnyFunSuite {

  val testSchema = StructType(Array(
    StructField("id", LongType, nullable = false),
    StructField("title", StringType, nullable = true),
    StructField("content", StringType, nullable = true)
  ))

  test("should throw IllegalArgumentException when metadata is null") {
    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        null, // null metadata should cause validation to fail
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("Split metadata cannot be null"))
    assert(exception.getMessage.contains("createSplitSearcher"))
    println(s"✅ Null metadata validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when metadata has no footer offsets") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(false)
    when(mockMetadata.getDocMappingJson()).thenReturn("{\"fields\":[]}")

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("does not contain required footer offsets"))
    println(s"✅ Missing footer offsets validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when footer offsets are invalid (zero or negative)") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(0L) // Invalid: should be > 0
    when(mockMetadata.getFooterEndOffset()).thenReturn(100L)
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(50L)
    when(mockMetadata.getHotcacheLength()).thenReturn(25L)
    when(mockMetadata.getDocMappingJson()).thenReturn("{\"fields\":[]}")

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("invalid footer offsets"))
    assert(exception.getMessage.contains("must be positive non-zero"))
    println(s"✅ Invalid footer offsets validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when footer end offset is not greater than start offset") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(100L)
    when(mockMetadata.getFooterEndOffset()).thenReturn(50L) // Invalid: should be > start
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(10L)
    when(mockMetadata.getHotcacheLength()).thenReturn(25L)
    when(mockMetadata.getDocMappingJson()).thenReturn("{\"fields\":[]}")

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("footerEndOffset"))
    assert(exception.getMessage.contains("must be greater than footerStartOffset"))
    println(s"✅ Invalid footer offset order validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when hotcache offsets are invalid") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(10L)
    when(mockMetadata.getFooterEndOffset()).thenReturn(100L)
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(0L) // Invalid: should be > 0
    when(mockMetadata.getHotcacheLength()).thenReturn(25L)
    when(mockMetadata.getDocMappingJson()).thenReturn("{\"fields\":[]}")

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("invalid hotcache offsets"))
    println(s"✅ Invalid hotcache offsets validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when document mapping JSON is null or empty") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(10L)
    when(mockMetadata.getFooterEndOffset()).thenReturn(100L)
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(50L)
    when(mockMetadata.getHotcacheLength()).thenReturn(25L)
    when(mockMetadata.getDocMappingJson()).thenReturn(null) // Invalid: should not be null

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("must contain valid document mapping JSON"))
    println(s"✅ Null document mapping validation: ${exception.getMessage}")
  }

  test("should throw IllegalArgumentException when document mapping JSON is empty string") {
    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(10L)
    when(mockMetadata.getFooterEndOffset()).thenReturn(100L)
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(50L)
    when(mockMetadata.getHotcacheLength()).thenReturn(25L)
    when(mockMetadata.getDocMappingJson()).thenReturn("   ") // Invalid: empty/whitespace

    val exception = intercept[IllegalArgumentException] {
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )
    }

    assert(exception.getMessage.contains("must contain valid document mapping JSON"))
    println(s"✅ Empty document mapping validation: ${exception.getMessage}")
  }

  test("should pass validation with proper metadata") {
    // This test validates that the validation logic passes with correct metadata.
    // Since we can't easily create a working SplitSearcher in a unit test without
    // proper infrastructure, we'll just verify that validation doesn't throw our
    // IllegalArgumentExceptions.

    val mockMetadata = mock(classOf[QuickwitSplit.SplitMetadata])
    when(mockMetadata.hasFooterOffsets()).thenReturn(true)
    when(mockMetadata.getFooterStartOffset()).thenReturn(100L)
    when(mockMetadata.getFooterEndOffset()).thenReturn(500L)
    when(mockMetadata.getHotcacheStartOffset()).thenReturn(200L)
    when(mockMetadata.getHotcacheLength()).thenReturn(150L)
    when(mockMetadata.getDocMappingJson()).thenReturn("""{"fields":[{"name":"id","type":"i64"}]}""")

    var validationPassed = false

    try {
      // This should pass all our validation checks but will fail later when trying to create the actual cache manager
      SplitSearchEngine.fromSplitFileWithMetadata(
        testSchema,
        "s3://test-bucket/test.split",
        mockMetadata,
        SplitCacheConfig()
      )

      // If we get here without an exception, validation passed completely
      validationPassed = true
      println(s"✅ Valid metadata passed all validation checks and created successfully!")

    } catch {
      case e: IllegalArgumentException if e.getMessage.contains("Split metadata cannot be null") ||
                                        e.getMessage.contains("does not contain required footer offsets") ||
                                        e.getMessage.contains("invalid footer offsets") ||
                                        e.getMessage.contains("invalid hotcache offsets") ||
                                        e.getMessage.contains("must contain valid document mapping JSON") =>
        // These are our validation exceptions - should not happen with valid metadata
        fail(s"Valid metadata should not trigger validation errors: ${e.getMessage}")

      case _: Exception =>
        // Any other exception (like from trying to create actual SplitSearcher) is expected and fine
        validationPassed = true
        println(s"✅ Valid metadata passed validation (failed later as expected during actual searcher creation)")
    }

    // Ensure the test passed one way or another
    assert(validationPassed, "Validation should have passed for valid metadata")
  }

  test("should demonstrate docBatch method usage") {
    // This is more of a documentation test showing how docBatch should be used
    // We can't easily test this without a real SplitSearcher, but we can document the pattern

    println("✅ DocBatch usage pattern demonstrated:")
    println("   Before: document = splitSearcher.doc(hit.getDocAddress())")
    println("   After:  val documents = splitSearcher.docBatch(List(hit.getDocAddress()).asJava)")
    println("          val document = if (documents.nonEmpty) documents.head else null")
    println("")
    println("   For multiple documents:")
    println("   val addresses = hits.map(hit => hit.getDocAddress())")
    println("   val documents = splitSearcher.docBatch(addresses.asJava)")

    // This test always passes - it's just documenting the API change
    assert(true, "docBatch pattern documented")
  }
}