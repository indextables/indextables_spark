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

package io.indextables.spark.core

import org.scalatest.funsuite.AnyFunSuite

/** Test to discover the correct aggregate expression classes in Spark 3.5. */
class AggregateExpressionTest extends AnyFunSuite {

  test("discover aggregate expression classes") {
    // Print available classes in aggregate package
    try {
      val aggClazz = Class.forName("org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc")
      println(s"✅ AggregateFunc available: ${aggClazz.getName}")

      val methods = aggClazz.getMethods.map(_.getName).distinct.sorted
      println(s"AggregateFunc methods: ${methods.mkString(", ")}")
    } catch {
      case e: Exception => println(s"❌ Error loading AggregateFunc: ${e.getMessage}")
    }

    // Try to find specific aggregate types
    val aggregateTypes = Seq(
      "org.apache.spark.sql.connector.expressions.aggregate.Count",
      "org.apache.spark.sql.connector.expressions.aggregate.Sum",
      "org.apache.spark.sql.connector.expressions.aggregate.Average",
      "org.apache.spark.sql.connector.expressions.aggregate.Avg",
      "org.apache.spark.sql.connector.expressions.aggregate.Min",
      "org.apache.spark.sql.connector.expressions.aggregate.Max"
    )

    aggregateTypes.foreach { className =>
      try {
        val clazz = Class.forName(className)
        println(s"✅ $className available")

        val methods = clazz.getMethods.map(_.getName).distinct.sorted
        println(s"  Methods: ${methods.mkString(", ")}")
      } catch {
        case _: ClassNotFoundException => println(s"❌ $className not found")
        case e: Exception              => println(s"❌ Error loading $className: ${e.getMessage}")
      }
    }

    // Try FieldReference
    try {
      val clazz = Class.forName("org.apache.spark.sql.connector.expressions.FieldReference")
      println(s"✅ FieldReference available: ${clazz.getName}")

      val methods = clazz.getMethods.map(_.getName).distinct.sorted
      println(s"FieldReference methods: ${methods.mkString(", ")}")
    } catch {
      case e: Exception => println(s"❌ Error loading FieldReference: ${e.getMessage}")
    }
  }
}
