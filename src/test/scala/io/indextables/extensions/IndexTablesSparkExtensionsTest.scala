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

package io.indextables.extensions

import org.apache.spark.sql.SparkSessionExtensions

import io.indextables.spark.extensions.IndexTables4SparkExtensions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/** Test suite to verify that IndexTablesSparkExtensions correctly aliases IndexTables4SparkExtensions functionality. */
class IndexTablesSparkExtensionsTest extends AnyFunSuite with Matchers {

  test("IndexTablesSparkExtensions should be instantiable") {
    // Verify the alias class can be instantiated
    val extensions = new IndexTablesSparkExtensions()
    extensions should not be null
  }

  test("IndexTablesSparkExtensions should be a subclass of IndexTables4SparkExtensions") {
    // Verify inheritance relationship
    val extensions = new IndexTablesSparkExtensions()
    extensions shouldBe a[IndexTables4SparkExtensions]
  }

  test("IndexTablesSparkExtensions should function as SparkSessionExtensions => Unit") {
    // Verify it can be used as a function
    val extensions = new IndexTablesSparkExtensions()
    extensions shouldBe a[Function1[_, _]]

    // Verify the function signature matches what Spark expects
    val func: SparkSessionExtensions => Unit = extensions
    func should not be null
  }

  test("Both extension classes should apply the same extensions") {
    // Create mock SparkSessionExtensions
    val sparkExtensions1 = new SparkSessionExtensions
    val sparkExtensions2 = new SparkSessionExtensions

    // Apply both extension classes
    val tantivy4Extensions    = new IndexTables4SparkExtensions()
    val indexTablesExtensions = new IndexTablesSparkExtensions()

    // Both should work without throwing exceptions
    noException should be thrownBy {
      tantivy4Extensions.apply(sparkExtensions1)
    }

    noException should be thrownBy {
      indexTablesExtensions.apply(sparkExtensions2)
    }
  }

  test("IndexTablesSparkExtensions can be used in spark.sql.extensions configuration") {
    // This test verifies the class can be loaded by name (as Spark does)
    val className = "io.indextables.extensions.IndexTablesSparkExtensions"

    // Load the class by name
    val clazz = Class.forName(className)
    clazz should not be null

    // Verify it's the right type
    classOf[IndexTables4SparkExtensions].isAssignableFrom(clazz) shouldBe true
    classOf[Function1[_, _]].isAssignableFrom(clazz) shouldBe true

    // Verify we can instantiate it
    val instance = clazz.getDeclaredConstructor().newInstance()
    instance shouldBe a[IndexTablesSparkExtensions]
  }
}
