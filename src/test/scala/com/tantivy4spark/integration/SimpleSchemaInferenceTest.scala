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

package com.tantivy4spark.integration

import com.tantivy4spark.config.{TantivyConfig, SchemaManager}
import com.tantivy4spark.search.TantivyIndexWriter
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import scala.util.{Success, Failure}
import java.io.File
import java.nio.file.Files

class SimpleSchemaInferenceTest extends AnyFlatSpec with Matchers with BeforeAndAfter {

  var tempDir: File = _
  
  before {
    tempDir = Files.createTempDirectory("simple-schema-test").toFile
    tempDir.deleteOnExit()
  }

  after {
    if (tempDir != null && tempDir.exists()) {
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir)
    }
  }

  "Schema inference" should "save schema during write operations" in {
    val testSchema = StructType(Array(
      StructField("user_id", LongType, nullable = false),
      StructField("username", StringType, nullable = true),
      StructField("email", StringType, nullable = false)
    ))

    val options = Map(
      "index.id" -> "test_user_index",
      "schema.path" -> tempDir.getAbsolutePath,
      "tantivy.base.path" -> tempDir.getAbsolutePath
    )

    val indexWriter = new TantivyIndexWriter(
      basePath = s"${tempDir.getAbsolutePath}/test_user_index",
      dataSchema = testSchema,
      options = options
    )

    // Create a sample row
    val row = InternalRow(
      123L, // user_id
      org.apache.spark.unsafe.types.UTF8String.fromString("testuser"), // username
      org.apache.spark.unsafe.types.UTF8String.fromString("test@example.com") // email
    )

    // Write the row (this should trigger schema inference and saving)
    val writeResult = indexWriter.writeRow(row)
    writeResult.recordCount should be (1)

    indexWriter.close()

    // Verify the schema was saved
    val schemaManager = new SchemaManager(options)
    schemaManager.schemaExists("test_user_index") should be (true)

    // Verify schema content
    schemaManager.loadSchema("test_user_index") match {
      case Success(config) =>
        val savedSchema = TantivyConfig.toSparkSchema(config)
        savedSchema.fields.length should be >= testSchema.fields.length
        
        // Check that all original fields are present
        testSchema.fields.foreach { originalField =>
          val savedField = savedSchema.fields.find(_.name == originalField.name)
          savedField should be (defined)
        }
        
      case Failure(exception) =>
        fail(s"Failed to load saved schema: ${exception.getMessage}")
    }
  }

  it should "handle type mapping correctly" in {
    import org.apache.spark.sql.types._
    
    val testCases = Map(
      StringType -> "text",
      IntegerType -> "i32", 
      LongType -> "i64",
      DoubleType -> "f64",
      BooleanType -> "bool",
      TimestampType -> "datetime"
    )

    testCases.foreach { case (sparkType, expectedTantivyType) =>
      val tantivyType = TantivyConfig.mapSparkTypeToTantivy(sparkType)
      tantivyType should be (expectedTantivyType)
      
      val roundTripSparkType = TantivyConfig.mapTantivyTypeToSpark(tantivyType)
      roundTripSparkType should be (sparkType)
    }
  }
}