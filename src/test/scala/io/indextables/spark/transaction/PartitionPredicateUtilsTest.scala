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
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

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
}
