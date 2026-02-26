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

import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{And => CatalystAnd, Not => CatalystNot, Or => CatalystOr}
import org.apache.spark.sql.catalyst.expressions.{EqualTo => CatalystEqualTo, GreaterThan => CatalystGreaterThan}
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.catalyst.expressions.{GreaterThanOrEqual => CatalystGreaterThanOrEqual}
import org.apache.spark.sql.catalyst.expressions.{
  In => CatalystIn,
  IsNotNull => CatalystIsNotNull,
  IsNull => CatalystIsNull
}
import org.apache.spark.sql.catalyst.expressions.{
  LessThan => CatalystLessThan,
  LessThanOrEqual => CatalystLessThanOrEqual
}
import org.apache.spark.sql.catalyst.expressions.{StartsWith => CatalystStartsWith}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.transaction.AddAction
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for PartitionPredicateUtils.expressionsToFilters() and expressionToFilter().
 *
 * These methods convert Catalyst Expression objects to Spark Filter objects for Avro manifest pruning in commands like
 * PREWARM, DROP PARTITIONS, and MERGE SPLITS.
 *
 * Run with: mvn scalatest:test -DwildcardSuites='io.indextables.spark.transaction.PartitionPredicateUtilsTest'
 */
class PartitionPredicateUtilsTest extends AnyFunSuite with Matchers {

  // Helper to create UTF8String literals (how Spark stores string literals internally)
  private def utf8(s: String): UTF8String = UTF8String.fromString(s)

  // Helper to verify EqualTo filter with any value type
  private def assertEqualToFilter(
    filter: Option[Filter],
    expectedAttr: String,
    expectedValue: Any
  ): Unit = {
    filter shouldBe defined
    filter.get match {
      case EqualTo(attr, value) =>
        attr shouldBe expectedAttr
        value.toString shouldBe expectedValue.toString
      case other => fail(s"Expected EqualTo filter, got: $other")
    }
  }

  // ==========================================================================
  // Equality Filter Conversion
  // ==========================================================================

  test("expressionToFilter should convert EqualTo with column on left") {
    val expr   = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01")))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    assertEqualToFilter(result, "date", "2024-01-01")
  }

  test("expressionToFilter should convert EqualTo with column on right") {
    val expr   = CatalystEqualTo(Literal(utf8("2024-01-01")), UnresolvedAttribute("date"))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    assertEqualToFilter(result, "date", "2024-01-01")
  }

  test("expressionToFilter should handle multi-part attribute names") {
    val expr   = CatalystEqualTo(UnresolvedAttribute(Seq("table", "column")), Literal(utf8("value")))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    assertEqualToFilter(result, "table.column", "value")
  }

  // ==========================================================================
  // Comparison Filter Conversion
  // ==========================================================================

  test("expressionToFilter should convert GreaterThan with column on left") {
    val expr   = CatalystGreaterThan(UnresolvedAttribute("score"), Literal(100))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case GreaterThan(attr, value) =>
        attr shouldBe "score"
        value shouldBe 100
      case other => fail(s"Expected GreaterThan filter, got: $other")
    }
  }

  test("expressionToFilter should convert GreaterThan with column on right (swap to LessThan)") {
    val expr   = CatalystGreaterThan(Literal(100), UnresolvedAttribute("score"))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case LessThan(attr, value) =>
        attr shouldBe "score"
        value shouldBe 100
      case other => fail(s"Expected LessThan filter, got: $other")
    }
  }

  test("expressionToFilter should convert GreaterThanOrEqual") {
    val expr   = CatalystGreaterThanOrEqual(UnresolvedAttribute("value"), Literal(50))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case GreaterThanOrEqual(attr, value) =>
        attr shouldBe "value"
        value shouldBe 50
      case other => fail(s"Expected GreaterThanOrEqual filter, got: $other")
    }
  }

  test("expressionToFilter should convert LessThan") {
    val expr   = CatalystLessThan(UnresolvedAttribute("count"), Literal(1000))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case LessThan(attr, value) =>
        attr shouldBe "count"
        value shouldBe 1000
      case other => fail(s"Expected LessThan filter, got: $other")
    }
  }

  test("expressionToFilter should convert LessThanOrEqual") {
    val expr   = CatalystLessThanOrEqual(UnresolvedAttribute("amount"), Literal(999.99))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case LessThanOrEqual(attr, value) =>
        attr shouldBe "amount"
        value shouldBe 999.99
      case other => fail(s"Expected LessThanOrEqual filter, got: $other")
    }
  }

  // ==========================================================================
  // IN Filter Conversion
  // ==========================================================================

  test("expressionToFilter should convert IN with all literal values") {
    val expr = CatalystIn(
      UnresolvedAttribute("region"),
      Seq(Literal(utf8("us-east")), Literal(utf8("us-west")), Literal(utf8("eu-west")))
    )
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case In(attr, values) =>
        attr shouldBe "region"
        values.map(_.toString) should contain allOf ("us-east", "us-west", "eu-west")
      case other => fail(s"Expected In filter, got: $other")
    }
  }

  test("expressionToFilter should return None for IN with non-literal values") {
    // IN with a subquery or other non-literal would not have all literals
    // This test simulates that by having an expression that's not a Literal
    val expr = CatalystIn(
      UnresolvedAttribute("id"),
      Seq(Literal(1), UnresolvedAttribute("other")) // mixed literals and non-literals
    )
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe None
  }

  // ==========================================================================
  // NULL Filter Conversion
  // ==========================================================================

  test("expressionToFilter should convert IsNull") {
    val expr   = CatalystIsNull(UnresolvedAttribute("optional_field"))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get shouldBe IsNull("optional_field")
  }

  test("expressionToFilter should convert IsNotNull") {
    val expr   = CatalystIsNotNull(UnresolvedAttribute("required_field"))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get shouldBe IsNotNull("required_field")
  }

  // ==========================================================================
  // String Pattern Filter Conversion
  // ==========================================================================

  test("expressionToFilter should convert StartsWith") {
    val expr   = CatalystStartsWith(UnresolvedAttribute("name"), Literal(utf8("prefix_")))
    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case StringStartsWith(attr, value) =>
        attr shouldBe "name"
        value shouldBe "prefix_"
      case other => fail(s"Expected StringStartsWith filter, got: $other")
    }
  }

  // ==========================================================================
  // Compound Filter Conversion (AND, OR, NOT)
  // ==========================================================================

  test("expressionToFilter should convert AND when both sides convert") {
    val left  = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01")))
    val right = CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us-east")))
    val expr  = CatalystAnd(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case And(EqualTo(attr1, val1), EqualTo(attr2, val2)) =>
        attr1 shouldBe "date"
        val1.toString shouldBe "2024-01-01"
        attr2 shouldBe "region"
        val2.toString shouldBe "us-east"
      case other => fail(s"Expected And(EqualTo, EqualTo) filter, got: $other")
    }
  }

  test("expressionToFilter should return partial AND when only left side converts") {
    val left = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01")))
    // Create an expression that won't convert (two literals)
    val right = CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b")))
    val expr  = CatalystAnd(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    // Should return just the left side that converted
    assertEqualToFilter(result, "date", "2024-01-01")
  }

  test("expressionToFilter should return partial AND when only right side converts") {
    val left  = CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b"))) // won't convert
    val right = CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us-east")))
    val expr  = CatalystAnd(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    assertEqualToFilter(result, "region", "us-east")
  }

  test("expressionToFilter should return None for AND when neither side converts") {
    val left  = CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b")))
    val right = CatalystEqualTo(Literal(utf8("c")), Literal(utf8("d")))
    val expr  = CatalystAnd(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe None
  }

  test("expressionToFilter should convert OR when both sides convert") {
    val left  = CatalystEqualTo(UnresolvedAttribute("status"), Literal(utf8("active")))
    val right = CatalystEqualTo(UnresolvedAttribute("status"), Literal(utf8("pending")))
    val expr  = CatalystOr(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case Or(EqualTo(attr1, val1), EqualTo(attr2, val2)) =>
        attr1 shouldBe "status"
        val1.toString shouldBe "active"
        attr2 shouldBe "status"
        val2.toString shouldBe "pending"
      case other => fail(s"Expected Or(EqualTo, EqualTo) filter, got: $other")
    }
  }

  test("expressionToFilter should return None for OR when only one side converts") {
    // OR requires both sides to convert (can't partially apply OR)
    val left  = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01")))
    val right = CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b"))) // won't convert
    val expr  = CatalystOr(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe None // OR requires both sides
  }

  test("expressionToFilter should convert NOT") {
    val inner = CatalystEqualTo(UnresolvedAttribute("deleted"), Literal(true))
    val expr  = CatalystNot(inner)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case Not(EqualTo(attr, value)) =>
        attr shouldBe "deleted"
        value shouldBe true
      case other => fail(s"Expected Not(EqualTo) filter, got: $other")
    }
  }

  test("expressionToFilter should return None for NOT when inner doesn't convert") {
    val inner = CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b"))) // won't convert
    val expr  = CatalystNot(inner)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe None
  }

  // ==========================================================================
  // expressionsToFilters (batch conversion)
  // ==========================================================================

  test("expressionsToFilters should convert multiple expressions") {
    val expressions = Seq(
      CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01"))),
      CatalystGreaterThan(UnresolvedAttribute("score"), Literal(50)),
      CatalystIsNotNull(UnresolvedAttribute("name"))
    )

    val result = PartitionPredicateUtils.expressionsToFilters(expressions)

    result should have size 3

    // Verify first filter is EqualTo
    result(0) match {
      case EqualTo(attr, value) =>
        attr shouldBe "date"
        value.toString shouldBe "2024-01-01"
      case other => fail(s"Expected EqualTo, got: $other")
    }

    // Verify second filter is GreaterThan
    result(1) match {
      case GreaterThan(attr, value) =>
        attr shouldBe "score"
        value shouldBe 50
      case other => fail(s"Expected GreaterThan, got: $other")
    }

    // Verify third filter is IsNotNull
    result(2) shouldBe IsNotNull("name")
  }

  test("expressionsToFilters should skip unconvertible expressions") {
    val expressions = Seq(
      CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01"))),
      CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b"))), // won't convert
      CatalystGreaterThan(UnresolvedAttribute("score"), Literal(50))
    )

    val result = PartitionPredicateUtils.expressionsToFilters(expressions)

    result should have size 2

    result(0) match {
      case EqualTo(attr, _) => attr shouldBe "date"
      case other            => fail(s"Expected EqualTo, got: $other")
    }

    result(1) match {
      case GreaterThan(attr, _) => attr shouldBe "score"
      case other                => fail(s"Expected GreaterThan, got: $other")
    }
  }

  test("expressionsToFilters should return empty for empty input") {
    val result = PartitionPredicateUtils.expressionsToFilters(Seq.empty)
    result shouldBe empty
  }

  test("expressionsToFilters should return empty when no expressions convert") {
    val expressions = Seq(
      CatalystEqualTo(Literal(utf8("a")), Literal(utf8("b"))),
      CatalystEqualTo(Literal(utf8("c")), Literal(utf8("d")))
    )

    val result = PartitionPredicateUtils.expressionsToFilters(expressions)

    result shouldBe empty
  }

  // ==========================================================================
  // Complex nested expressions
  // ==========================================================================

  test("expressionToFilter should handle deeply nested AND/OR") {
    // (date = '2024-01-01' AND region = 'us') OR (date = '2024-01-02' AND region = 'eu')
    val left = CatalystAnd(
      CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01"))),
      CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us")))
    )
    val right = CatalystAnd(
      CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-02"))),
      CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("eu")))
    )
    val expr = CatalystOr(left, right)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case Or(And(_, _), And(_, _)) => // structure is correct
      case other                    => fail(s"Expected Or(And, And), got: $other")
    }
  }

  test("expressionToFilter should handle NOT with compound expression") {
    // NOT (date = '2024-01-01' AND region = 'us')
    val inner = CatalystAnd(
      CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01"))),
      CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us")))
    )
    val expr = CatalystNot(inner)

    val result = PartitionPredicateUtils.expressionToFilter(expr)

    result shouldBe defined
    result.get match {
      case Not(And(_, _)) => // structure is correct
      case other          => fail(s"Expected Not(And), got: $other")
    }
  }

  // ==========================================================================
  // IT-036: Type-Aware Partition Schema (buildPartitionSchema)
  // ==========================================================================

  test("buildPartitionSchema should use StringType when no full schema provided") {
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("year", "month"))
    schema.length shouldBe 2
    schema(0).name shouldBe "year"
    schema(0).dataType shouldBe StringType
    schema(1).name shouldBe "month"
    schema(1).dataType shouldBe StringType
  }

  test("buildPartitionSchema should extract real types from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("year", IntegerType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("year", "month"), Some(fullSchema))
    schema.length shouldBe 2
    schema(0).name shouldBe "year"
    schema(0).dataType shouldBe IntegerType
    schema(1).name shouldBe "month"
    schema(1).dataType shouldBe IntegerType
  }

  test("buildPartitionSchema should fall back to StringType for unknown columns") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("year", IntegerType, nullable = true)
      )
    )
    // "month" not in full schema - should fall back to StringType
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("year", "month"), Some(fullSchema))
    schema(0).dataType shouldBe IntegerType
    schema(1).dataType shouldBe StringType
  }

  test("buildPartitionSchema should handle mixed types") {
    val fullSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("hour", IntegerType, nullable = true),
        StructField("region", StringType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("date", "hour", "region"), Some(fullSchema))
    schema(0).dataType shouldBe StringType
    schema(1).dataType shouldBe IntegerType
    schema(2).dataType shouldBe StringType
  }

  // ==========================================================================
  // IT-036: Type-Aware Row Conversion (createRowFromPartitionValues)
  // ==========================================================================

  test("createRowFromPartitionValues should convert integer partition values") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("month" -> "10"), partitionSchema)
    row.getInt(0) shouldBe 10
  }

  test("createRowFromPartitionValues should convert long partition values") {
    val partitionSchema = StructType(
      Seq(
        StructField("ts", LongType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("ts" -> "1234567890"), partitionSchema)
    row.getLong(0) shouldBe 1234567890L
  }

  test("createRowFromPartitionValues should keep string partition values as UTF8String") {
    val partitionSchema = StructType(
      Seq(
        StructField("region", StringType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("region" -> "us-east"), partitionSchema)
    row.getUTF8String(0).toString shouldBe "us-east"
  }

  test("createRowFromPartitionValues should handle null for missing partition columns") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map.empty, partitionSchema)
    row.isNullAt(0) shouldBe true
  }

  // ==========================================================================
  // IT-036: Type-Aware Expression Resolution (resolveExpression)
  // ==========================================================================

  test("resolveExpression should preserve literal types for typed schema") {
    val schema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    // The resolved expression should contain a BoundReference with IntegerType
    // and the literal should remain as IntegerType (not converted to StringType)
    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, IntegerType, _),
            Literal(9, IntegerType)
          ) => // correct
      case other => fail(s"Expected GreaterThan(BoundReference(IntegerType), Literal(9, IntegerType)), got: $other")
    }
  }

  test("resolveExpression should convert literals to string for all-string schema (backward compat)") {
    val schema = StructType(
      Seq(
        StructField("month", StringType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    // For all-string schema, the literal should be converted to UTF8String
    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, StringType, _),
            Literal(_, StringType)
          ) => // correct - literal was converted to StringType
      case other => fail(s"Expected string-converted literal for all-string schema, got: $other")
    }
  }

  // ==========================================================================
  // IT-036: Numeric Comparison Regression Tests (evaluatePredicates)
  // These tests would FAIL with the old code (lexicographic) and PASS with the fix.
  // ==========================================================================

  test("IT-036 REGRESSION: GreaterThan with integer month > 9 should match months 10, 11, 12") {
    // BUG: With lexicographic comparison, "10" < "9" because '1' < '9'
    // FIX: With type-aware comparison, 10 > 9 is correctly true
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))

    // These should match (month > 9)
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "10"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "11"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "12"), partitionSchema, Seq(predicate)) shouldBe true

    // These should NOT match (month <= 9)
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "9"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "1"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "5"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036 REGRESSION: LessThan with integer month < 10 should match months 1-9 only") {
    // BUG: With lexicographic comparison, "10" < "10" is false but "11" < "10" is also false
    // while "2" < "10" is false (because '2' > '1')
    // FIX: With type-aware comparison, correct numeric ordering
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystLessThan(UnresolvedAttribute("month"), Literal(10))

    // These should match (month < 10)
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "1"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "5"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "9"), partitionSchema, Seq(predicate)) shouldBe true

    // These should NOT match (month >= 10)
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "10"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "11"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "12"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036 REGRESSION: GreaterThanOrEqual with integer month >= 10 boundary test") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThanOrEqual(UnresolvedAttribute("month"), Literal(10))

    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "10"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "11"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "12"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "9"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "1"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036 REGRESSION: LessThanOrEqual with integer month <= 9 boundary test") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystLessThanOrEqual(UnresolvedAttribute("month"), Literal(9))

    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "1"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "5"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "9"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "10"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "11"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036 REGRESSION: BETWEEN equivalent with integer months 2-11 (AND of >= and <=)") {
    // WHERE month BETWEEN 2 AND 11  =>  month >= 2 AND month <= 11
    // BUG: "10" and "11" would be missed because "10" < "2" lexicographically
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predGte    = CatalystGreaterThanOrEqual(UnresolvedAttribute("month"), Literal(2))
    val predLte    = CatalystLessThanOrEqual(UnresolvedAttribute("month"), Literal(11))
    val predicates = Seq(predGte, predLte) // Both must be true (AND semantics in evaluatePredicates)

    // These should match: months 2 through 11
    for (m <- 2 to 11)
      withClue(s"month=$m should match BETWEEN 2 AND 11: ") {
        PartitionPredicateUtils.evaluatePredicates(
          Map("month" -> m.toString),
          partitionSchema,
          predicates
        ) shouldBe true
      }

    // These should NOT match: months 1 and 12
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "1"), partitionSchema, predicates) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "12"), partitionSchema, predicates) shouldBe false
  }

  test("IT-036 REGRESSION: Three-digit numeric boundary (99 vs 100)") {
    // Tests the digit-boundary crossing at 99->100
    val partitionSchema = StructType(
      Seq(
        StructField("partition_id", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("partition_id"), Literal(99))

    PartitionPredicateUtils.evaluatePredicates(
      Map("partition_id" -> "100"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("partition_id" -> "101"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("partition_id" -> "99"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("partition_id" -> "50"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036: Equality still works with typed schema") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("month"), Literal(10))

    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "10"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "9"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("month" -> "11"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036: String partition columns still work with string literals") {
    // Ensure backward compatibility - string partitions like date = '2024-01-01' still work
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-01-01")))

    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-02"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036: String date comparison still uses lexicographic ordering") {
    // Dates as strings: lexicographic comparison is correct for YYYY-MM-DD format
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("date"), Literal(utf8("2024-06-01")))

    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-07-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-12-31"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-05-31"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-06-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036: filterAddActionsByPredicates with typed schema") {
    val partitionSchema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))

    // Create mock AddActions with different month partition values
    val actions = (1 to 12).map { m =>
      AddAction(
        path = s"split_month_$m.split",
        partitionValues = Map("month" -> m.toString),
        size = 1000L,
        modificationTime = System.currentTimeMillis(),
        dataChange = true
      )
    }

    val filtered = PartitionPredicateUtils.filterAddActionsByPredicates(actions, partitionSchema, Seq(predicate))

    // Should only include months 10, 11, 12
    filtered.length shouldBe 3
    filtered.map(_.partitionValues("month")).toSet shouldBe Set("10", "11", "12")
  }

  // ==========================================================================
  // IT-036: Mixed-Type Schema Edge Cases (Code Review Finding #1)
  // Tests verifying that resolveExpression handles mixed-type schemas correctly,
  // including cross-type comparisons where a literal's type doesn't match
  // the column's type.
  // ==========================================================================

  test("IT-036 MIXED-TYPE: integer comparison works correctly in mixed-type schema") {
    // Schema has both StringType and IntegerType columns
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))

    // Numeric comparison should work correctly even though schema is mixed-type
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "10"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "11"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "9"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "5"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 MIXED-TYPE: string comparison works correctly in mixed-type schema") {
    // Schema has both StringType and IntegerType columns
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-06-15")))

    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-06-15", "month" -> "6"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-07-01", "month" -> "7"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 MIXED-TYPE: cross-type comparison coerces literal to column type") {
    // Edge case: integer literal compared against a StringType column in a mixed schema.
    // The column-context-aware resolveExpression should coerce the integer literal to
    // a UTF8String so that the comparison is type-safe.
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    // WHERE date > 5 - comparing a string column to an integer literal
    val predicate = CatalystGreaterThan(UnresolvedAttribute("date"), Literal(5))

    // After coercion, this becomes: date > "5" (string comparison)
    // "2024-01-01" > "5" is lexicographically false ('2' < '5')
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "1"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false

    // "9" > "5" is lexicographically true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "9", "month" -> "1"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true

    // "5" > "5" is false (not strictly greater)
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "5", "month" -> "1"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 MIXED-TYPE: combined predicates on both column types in mixed schema") {
    // Test WHERE date = '2024-10-15' AND month > 9 on a mixed-type schema
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predDate  = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-10-15")))
    val predMonth = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))

    // Both predicates satisfied
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-10-15", "month" -> "10"),
      partitionSchema,
      Seq(predDate, predMonth)
    ) shouldBe true

    // Date matches, month doesn't
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-10-15", "month" -> "9"),
      partitionSchema,
      Seq(predDate, predMonth)
    ) shouldBe false

    // Month matches, date doesn't
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-11-01", "month" -> "11"),
      partitionSchema,
      Seq(predDate, predMonth)
    ) shouldBe false

    // Neither matches
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-01-01", "month" -> "1"),
      partitionSchema,
      Seq(predDate, predMonth)
    ) shouldBe false
  }

  test("IT-036 MIXED-TYPE: resolveExpression coerces integer literal for StringType column") {
    // Verify the resolved expression has matching types (no type mismatch)
    val schema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("date"), Literal(5))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, StringType, _),
            Literal(_, StringType)
          ) => // correct: literal was coerced from IntegerType to StringType
      case other => fail(s"Expected GreaterThan(BoundReference(StringType), Literal(_, StringType)), got: $other")
    }
  }

  test("IT-036 MIXED-TYPE: resolveExpression preserves integer literal for IntegerType column") {
    // Verify the resolved expression keeps integer literal for integer column
    val schema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(1, IntegerType, _),
            Literal(9, IntegerType)
          ) => // correct: literal stays as IntegerType
      case other => fail(s"Expected GreaterThan(BoundReference(IntegerType), Literal(9, IntegerType)), got: $other")
    }
  }

  test("IT-036 MIXED-TYPE: resolveExpression coerces string literal to IntegerType for integer column") {
    // When a string literal like "9" is compared against an integer column,
    // the literal should be coerced to IntegerType
    val schema = StructType(
      Seq(
        StructField("month", IntegerType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(utf8("9"), StringType))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, IntegerType, _),
            Literal(9, IntegerType)
          ) => // correct: string literal "9" was coerced to integer 9
      case other => fail(s"Expected GreaterThan(BoundReference(IntegerType), Literal(9, IntegerType)), got: $other")
    }
  }

  test("IT-036 MIXED-TYPE: IN expression coerces literals in mixed-type schema") {
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predicate = CatalystIn(
      UnresolvedAttribute("month"),
      Seq(Literal(10), Literal(11), Literal(12))
    )

    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-10-01", "month" -> "10"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-11-01", "month" -> "11"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("date" -> "2024-09-01", "month" -> "9"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 MIXED-TYPE: filterAddActionsByPredicates with mixed-type schema") {
    val partitionSchema = StructType(
      Seq(
        StructField("date", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true)
      )
    )
    val predDate  = CatalystEqualTo(UnresolvedAttribute("date"), Literal(utf8("2024-10-15")))
    val predMonth = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))

    val actions = Seq(
      AddAction("split_1.split", Map("date" -> "2024-10-15", "month" -> "10"), 1000L, System.currentTimeMillis(), true),
      AddAction("split_2.split", Map("date" -> "2024-10-15", "month" -> "9"), 1000L, System.currentTimeMillis(), true),
      AddAction("split_3.split", Map("date" -> "2024-11-01", "month" -> "11"), 1000L, System.currentTimeMillis(), true),
      AddAction("split_4.split", Map("date" -> "2024-10-15", "month" -> "11"), 1000L, System.currentTimeMillis(), true)
    )

    val filtered =
      PartitionPredicateUtils.filterAddActionsByPredicates(actions, partitionSchema, Seq(predDate, predMonth))

    // Only split_1 (date matches, month 10 > 9) and split_4 (date matches, month 11 > 9) should match
    filtered.length shouldBe 2
    filtered.map(_.path).toSet shouldBe Set("split_1.split", "split_4.split")
  }

  // ==========================================================================
  // IT-036: buildPartitionSchema tests for additional types
  // (Boolean, Short, Byte, Date, Timestamp)
  // ==========================================================================

  test("IT-036: buildPartitionSchema should extract BooleanType from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("is_active"), Some(fullSchema))
    schema.length shouldBe 1
    schema(0).name shouldBe "is_active"
    schema(0).dataType shouldBe BooleanType
  }

  test("IT-036: buildPartitionSchema should extract ShortType from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("priority", ShortType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("priority"), Some(fullSchema))
    schema.length shouldBe 1
    schema(0).name shouldBe "priority"
    schema(0).dataType shouldBe ShortType
  }

  test("IT-036: buildPartitionSchema should extract ByteType from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("level", ByteType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("level"), Some(fullSchema))
    schema.length shouldBe 1
    schema(0).name shouldBe "level"
    schema(0).dataType shouldBe ByteType
  }

  test("IT-036: buildPartitionSchema should extract DateType from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("event_date", DateType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("event_date"), Some(fullSchema))
    schema.length shouldBe 1
    schema(0).name shouldBe "event_date"
    schema(0).dataType shouldBe DateType
  }

  test("IT-036: buildPartitionSchema should extract TimestampType from full schema") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("created_at", TimestampType, nullable = true)
      )
    )
    val schema = PartitionPredicateUtils.buildPartitionSchema(Seq("created_at"), Some(fullSchema))
    schema.length shouldBe 1
    schema(0).name shouldBe "created_at"
    schema(0).dataType shouldBe TimestampType
  }

  test("IT-036: buildPartitionSchema should handle mixed schema with all supported types") {
    val fullSchema = StructType(
      Seq(
        StructField("id", IntegerType, nullable = false),
        StructField("name", StringType, nullable = true),
        StructField("is_active", BooleanType, nullable = true),
        StructField("priority", ShortType, nullable = true),
        StructField("level", ByteType, nullable = true),
        StructField("event_date", DateType, nullable = true),
        StructField("created_at", TimestampType, nullable = true),
        StructField("score", DoubleType, nullable = true)
      )
    )
    val partCols = Seq("is_active", "priority", "level", "event_date", "created_at", "score")
    val schema   = PartitionPredicateUtils.buildPartitionSchema(partCols, Some(fullSchema))
    schema.length shouldBe 6
    schema(0).dataType shouldBe BooleanType
    schema(1).dataType shouldBe ShortType
    schema(2).dataType shouldBe ByteType
    schema(3).dataType shouldBe DateType
    schema(4).dataType shouldBe TimestampType
    schema(5).dataType shouldBe DoubleType
  }

  // ==========================================================================
  // IT-036: createRowFromPartitionValues tests for additional types
  // (Boolean, Short, Byte, Date, Timestamp)
  // ==========================================================================

  test("IT-036: createRowFromPartitionValues should convert boolean 'true' partition value") {
    val partitionSchema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("is_active" -> "true"), partitionSchema)
    row.getBoolean(0) shouldBe true
  }

  test("IT-036: createRowFromPartitionValues should convert boolean 'false' partition value") {
    val partitionSchema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("is_active" -> "false"), partitionSchema)
    row.getBoolean(0) shouldBe false
  }

  test("IT-036: createRowFromPartitionValues should convert short partition value") {
    val partitionSchema = StructType(
      Seq(
        StructField("priority", ShortType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("priority" -> "100"), partitionSchema)
    row.getShort(0) shouldBe 100.toShort
  }

  test("IT-036: createRowFromPartitionValues should convert byte partition value") {
    val partitionSchema = StructType(
      Seq(
        StructField("level", ByteType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("level" -> "42"), partitionSchema)
    row.getByte(0) shouldBe 42.toByte
  }

  test("IT-036: createRowFromPartitionValues should convert date partition value to epoch day int") {
    val partitionSchema = StructType(
      Seq(
        StructField("event_date", DateType, nullable = true)
      )
    )
    val row = PartitionPredicateUtils.createRowFromPartitionValues(Map("event_date" -> "2024-01-15"), partitionSchema)
    val expectedEpochDay = java.time.LocalDate.parse("2024-01-15").toEpochDay.toInt
    row.getInt(0) shouldBe expectedEpochDay
  }

  test("IT-036: createRowFromPartitionValues should convert timestamp partition value to microseconds") {
    val partitionSchema = StructType(
      Seq(
        StructField("created_at", TimestampType, nullable = true)
      )
    )
    val row =
      PartitionPredicateUtils.createRowFromPartitionValues(Map("created_at" -> "2024-01-15T10:30:00Z"), partitionSchema)
    val instant = java.time.Instant.parse("2024-01-15T10:30:00Z")
    val expectedMicros = java.time.Duration.between(java.time.Instant.EPOCH, instant).getSeconds * 1000000L +
      instant.getNano / 1000L
    row.getLong(0) shouldBe expectedMicros
  }

  // ==========================================================================
  // IT-036: resolveExpression / coerceLiteralToType tests for additional types
  // Verifies that type coercion works or gracefully handles unsupported types.
  // ==========================================================================

  test("IT-036: resolveExpression should preserve boolean literal for BooleanType column") {
    // Boolean literal matching a BooleanType column - no coercion needed
    val schema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val expr     = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(true))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystEqualTo(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, BooleanType, _),
            Literal(true, BooleanType)
          ) => // correct: boolean literal preserved for boolean column
      case other => fail(s"Expected EqualTo(BoundReference(BooleanType), Literal(true, BooleanType)), got: $other")
    }
  }

  test("IT-036: resolveExpression should coerce string 'true' to boolean for BooleanType column") {
    // String literal "true" compared to BooleanType column
    // coerceLiteralToType handles BooleanType coercion: converts "true"/"false" strings to Boolean.
    val schema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val expr     = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(utf8("true"), StringType))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    // coerceLiteralToType converts string "true" to boolean true with BooleanType.
    resolved match {
      case CatalystEqualTo(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, BooleanType, _),
            Literal(true, BooleanType)
          ) => // correct: string "true" coerced to boolean true
      case other => fail(s"Expected EqualTo(BoundReference(BooleanType), Literal(true, BooleanType)), got: $other")
    }
  }

  test("IT-036: resolveExpression should coerce int literal to ShortType column") {
    // Integer literal compared to ShortType column
    // coerceLiteralToType handles ShortType coercion: converts integer string to Short.
    val schema = StructType(
      Seq(
        StructField("priority", ShortType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("priority"), Literal(100))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, ShortType, _),
            Literal(_, ShortType)
          ) => // correct: integer literal coerced to ShortType
      case other => fail(s"Expected GreaterThan(BoundReference(ShortType), Literal(_, ShortType)), got: $other")
    }
  }

  test("IT-036: resolveExpression should coerce int literal to ByteType column") {
    // Integer literal compared to ByteType column
    // coerceLiteralToType handles ByteType coercion: converts integer string to Byte.
    val schema = StructType(
      Seq(
        StructField("level", ByteType, nullable = true)
      )
    )
    val expr     = CatalystEqualTo(UnresolvedAttribute("level"), Literal(5))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystEqualTo(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, ByteType, _),
            Literal(_, ByteType)
          ) => // correct: integer literal coerced to ByteType
      case other => fail(s"Expected EqualTo(BoundReference(ByteType), Literal(_, ByteType)), got: $other")
    }
  }

  test("IT-036: resolveExpression should coerce string literal to DateType column") {
    // String literal compared to DateType column
    // coerceLiteralToType handles DateType coercion: converts date string to epoch day Int.
    val schema = StructType(
      Seq(
        StructField("event_date", DateType, nullable = true)
      )
    )
    val expr     = CatalystGreaterThan(UnresolvedAttribute("event_date"), Literal(utf8("2024-06-01"), StringType))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, DateType, _),
            Literal(_, DateType)
          ) => // correct: string "2024-06-01" coerced to epoch day Int with DateType
      case other => fail(s"Expected GreaterThan(BoundReference(DateType), Literal(_, DateType)), got: $other")
    }
  }

  test("IT-036: resolveExpression should coerce string literal to TimestampType column") {
    // String literal compared to TimestampType column
    // coerceLiteralToType handles TimestampType coercion: converts timestamp string to microseconds Long.
    val schema = StructType(
      Seq(
        StructField("created_at", TimestampType, nullable = true)
      )
    )
    val expr = CatalystGreaterThan(UnresolvedAttribute("created_at"), Literal(utf8("2024-01-01T00:00:00Z"), StringType))
    val resolved = PartitionPredicateUtils.resolveExpression(expr, schema)

    resolved match {
      case CatalystGreaterThan(
            org.apache.spark.sql.catalyst.expressions.BoundReference(0, TimestampType, _),
            Literal(_, TimestampType)
          ) => // correct: string timestamp coerced to microseconds Long with TimestampType
      case other => fail(s"Expected GreaterThan(BoundReference(TimestampType), Literal(_, TimestampType)), got: $other")
    }
  }

  // ==========================================================================
  // IT-036: evaluatePredicates regression tests for additional types
  // Boolean, Short, Byte, Date, Timestamp
  // ==========================================================================

  test("IT-036: evaluatePredicates with BooleanType - equality matching") {
    val partitionSchema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(true))

    PartitionPredicateUtils.evaluatePredicates(
      Map("is_active" -> "true"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("is_active" -> "false"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036: evaluatePredicates with BooleanType - false equality") {
    val partitionSchema = StructType(
      Seq(
        StructField("is_active", BooleanType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(false))

    PartitionPredicateUtils.evaluatePredicates(
      Map("is_active" -> "false"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("is_active" -> "true"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 REGRESSION: ShortType comparison with boundary values") {
    val partitionSchema = StructType(
      Seq(
        StructField("priority", ShortType, nullable = true)
      )
    )
    val predicate = CatalystGreaterThan(UnresolvedAttribute("priority"), Literal(100.toShort, ShortType))

    // Should match: values > 100
    PartitionPredicateUtils.evaluatePredicates(Map("priority" -> "101"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("priority" -> "200"), partitionSchema, Seq(predicate)) shouldBe true

    // Should NOT match: values <= 100
    PartitionPredicateUtils.evaluatePredicates(Map("priority" -> "100"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("priority" -> "50"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("priority" -> "1"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036: ByteType equality matching") {
    val partitionSchema = StructType(
      Seq(
        StructField("level", ByteType, nullable = true)
      )
    )
    val predicate = CatalystEqualTo(UnresolvedAttribute("level"), Literal(5.toByte, ByteType))

    PartitionPredicateUtils.evaluatePredicates(Map("level" -> "5"), partitionSchema, Seq(predicate)) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(Map("level" -> "4"), partitionSchema, Seq(predicate)) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(Map("level" -> "6"), partitionSchema, Seq(predicate)) shouldBe false
  }

  test("IT-036 REGRESSION: DateType comparison with date boundaries") {
    val partitionSchema = StructType(
      Seq(
        StructField("event_date", DateType, nullable = true)
      )
    )
    // Use an integer literal matching the epoch day value for 2024-06-01
    val epochDay2024_06_01 = java.time.LocalDate.parse("2024-06-01").toEpochDay.toInt
    val predicate = CatalystGreaterThan(UnresolvedAttribute("event_date"), Literal(epochDay2024_06_01, DateType))

    // Should match: dates after 2024-06-01
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2024-06-02"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2024-12-31"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2025-01-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true

    // Should NOT match: dates on or before 2024-06-01
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2024-06-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2024-01-01"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("event_date" -> "2023-12-31"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  test("IT-036 REGRESSION: TimestampType comparison with timestamp boundaries") {
    val partitionSchema = StructType(
      Seq(
        StructField("created_at", TimestampType, nullable = true)
      )
    )
    // Use a long literal matching microseconds since epoch for 2024-01-01T00:00:00Z
    val instant2024 = java.time.Instant.parse("2024-01-01T00:00:00Z")
    val micros2024  = java.time.Duration.between(java.time.Instant.EPOCH, instant2024).getSeconds * 1000000L
    val predicate   = CatalystGreaterThan(UnresolvedAttribute("created_at"), Literal(micros2024, TimestampType))

    // Should match: timestamps after 2024-01-01T00:00:00Z
    PartitionPredicateUtils.evaluatePredicates(
      Map("created_at" -> "2024-06-15T12:00:00Z"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true
    PartitionPredicateUtils.evaluatePredicates(
      Map("created_at" -> "2024-01-01T00:00:01Z"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe true

    // Should NOT match: timestamps on or before 2024-01-01T00:00:00Z
    PartitionPredicateUtils.evaluatePredicates(
      Map("created_at" -> "2024-01-01T00:00:00Z"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
    PartitionPredicateUtils.evaluatePredicates(
      Map("created_at" -> "2023-12-31T23:59:59Z"),
      partitionSchema,
      Seq(predicate)
    ) shouldBe false
  }

  // ==========================================================================
  // IT-036: Mixed-type schema with all supported types
  // Combined predicates across String, Int, Boolean, Date columns
  // ==========================================================================

  test("IT-036: combined predicates on String, Int, Boolean, Date columns") {
    val partitionSchema = StructType(
      Seq(
        StructField("region", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true),
        StructField("is_active", BooleanType, nullable = true),
        StructField("event_date", DateType, nullable = true)
      )
    )

    val predRegion         = CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us-east")))
    val predMonth          = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(6))
    val predActive         = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(true))
    val epochDay2024_06_01 = java.time.LocalDate.parse("2024-06-01").toEpochDay.toInt
    val predDate = CatalystGreaterThan(UnresolvedAttribute("event_date"), Literal(epochDay2024_06_01, DateType))

    val predicates = Seq(predRegion, predMonth, predActive, predDate)

    // All predicates satisfied
    PartitionPredicateUtils.evaluatePredicates(
      Map("region" -> "us-east", "month" -> "10", "is_active" -> "true", "event_date" -> "2024-07-15"),
      partitionSchema,
      predicates
    ) shouldBe true

    // Region doesn't match
    PartitionPredicateUtils.evaluatePredicates(
      Map("region" -> "eu-west", "month" -> "10", "is_active" -> "true", "event_date" -> "2024-07-15"),
      partitionSchema,
      predicates
    ) shouldBe false

    // Month doesn't match (6 is not > 6)
    PartitionPredicateUtils.evaluatePredicates(
      Map("region" -> "us-east", "month" -> "6", "is_active" -> "true", "event_date" -> "2024-07-15"),
      partitionSchema,
      predicates
    ) shouldBe false

    // is_active doesn't match
    PartitionPredicateUtils.evaluatePredicates(
      Map("region" -> "us-east", "month" -> "10", "is_active" -> "false", "event_date" -> "2024-07-15"),
      partitionSchema,
      predicates
    ) shouldBe false

    // Date doesn't match (2024-05-15 is not > 2024-06-01)
    PartitionPredicateUtils.evaluatePredicates(
      Map("region" -> "us-east", "month" -> "10", "is_active" -> "true", "event_date" -> "2024-05-15"),
      partitionSchema,
      predicates
    ) shouldBe false
  }

  test("IT-036: filterAddActionsByPredicates with all-types mixed schema") {
    val partitionSchema = StructType(
      Seq(
        StructField("region", StringType, nullable = true),
        StructField("month", IntegerType, nullable = true),
        StructField("is_active", BooleanType, nullable = true)
      )
    )

    val predRegion = CatalystEqualTo(UnresolvedAttribute("region"), Literal(utf8("us-east")))
    val predMonth  = CatalystGreaterThan(UnresolvedAttribute("month"), Literal(9))
    val predActive = CatalystEqualTo(UnresolvedAttribute("is_active"), Literal(true))

    val actions = Seq(
      AddAction(
        "split_1.split",
        Map("region" -> "us-east", "month" -> "10", "is_active" -> "true"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "split_2.split",
        Map("region" -> "us-east", "month" -> "10", "is_active" -> "false"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "split_3.split",
        Map("region" -> "us-east", "month" -> "5", "is_active" -> "true"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "split_4.split",
        Map("region" -> "eu-west", "month" -> "11", "is_active" -> "true"),
        1000L,
        System.currentTimeMillis(),
        true
      ),
      AddAction(
        "split_5.split",
        Map("region" -> "us-east", "month" -> "12", "is_active" -> "true"),
        1000L,
        System.currentTimeMillis(),
        true
      )
    )

    val filtered = PartitionPredicateUtils.filterAddActionsByPredicates(
      actions,
      partitionSchema,
      Seq(predRegion, predMonth, predActive)
    )

    // Only split_1 (region=us-east, month=10>9, is_active=true) and split_5 (region=us-east, month=12>9, is_active=true) match
    filtered.length shouldBe 2
    filtered.map(_.path).toSet shouldBe Set("split_1.split", "split_5.split")
  }

  // Note: TimestampNTZType is available in Spark 3.4+ but is not currently used in
  // the codebase's partition handling. If TimestampNTZ partition support is needed,
  // it would require adding a case to convertPartitionValue and coerceLiteralToType.
}
