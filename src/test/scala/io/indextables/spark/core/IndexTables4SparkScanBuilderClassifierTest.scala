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

import org.apache.spark.sql.sources._

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for `IndexTables4SparkScanBuilder.classifyCandidateFilter`.
 *
 * These tests drive the classifier directly with `Filter` AST inputs, which lets us cover Catalyst-rewrite-
 * resistant cases (bare `Not(EqualTo)`, deeply nested trees, `Not(In)`) that end-to-end Spark tests cannot
 * reliably produce because Catalyst often rewrites top-level filters before they reach the data source.
 *
 * The load-bearing invariant: a candidate-classified filter is pushed to tantivy as an approximate superset
 * and Spark adds a row-level FilterExec for exact correctness. `Not` over any candidate inverts the superset
 * into a subset, which Spark cannot correct — so any tree containing a `Not` must be rejected.
 */
class IndexTables4SparkScanBuilderClassifierTest extends AnyFunSuite with Matchers {

  // Simulated typemap: `msg` is a text_and_string field (candidate-eligible), everything else is not.
  private val isCandidateField: String => Boolean = _.toLowerCase == "msg"

  private def classify(f: Filter): Boolean =
    IndexTables4SparkScanBuilder.classifyCandidateFilter(f, isCandidateField)

  // ═══════════════════════════════════════════════════════════════════
  //  Leaf filters — positive cases
  // ═══════════════════════════════════════════════════════════════════

  test("EqualTo on text_and_string field is a candidate") {
    classify(EqualTo("msg", "hello world")) shouldBe true
  }

  test("EqualNullSafe on text_and_string field is a candidate") {
    classify(EqualNullSafe("msg", "hello world")) shouldBe true
  }

  test("In on text_and_string field is a candidate") {
    classify(In("msg", Array[Any]("a", "b"))) shouldBe true
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Leaf filters — negative cases (not text_and_string)
  // ═══════════════════════════════════════════════════════════════════

  test("EqualTo on a non-candidate field is NOT a candidate") {
    classify(EqualTo("other", "hello world")) shouldBe false
  }

  test("EqualNullSafe on a non-candidate field is NOT a candidate") {
    classify(EqualNullSafe("other", "hello world")) shouldBe false
  }

  test("In on a non-candidate field is NOT a candidate") {
    classify(In("other", Array[Any]("a"))) shouldBe false
  }

  test("GreaterThan on a text_and_string field is NOT a candidate (range unsupported)") {
    classify(GreaterThan("msg", "m")) shouldBe false
  }

  test("StringStartsWith on a text_and_string field is NOT a candidate") {
    classify(StringStartsWith("msg", "foo")) shouldBe false
  }

  test("IsNull / IsNotNull are NOT candidates") {
    classify(IsNull("msg")) shouldBe false
    classify(IsNotNull("msg")) shouldBe false
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Critical: Not over any candidate must be rejected
  // ═══════════════════════════════════════════════════════════════════

  test("Not(EqualTo) on a text_and_string field is NOT a candidate") {
    // The round-1 review identified this as a critical wrong-results bug: a phrase-query candidate
    // is a superset of exact equality, and Not(superset) is a SUBSET of Not(exact) — which Spark's
    // post-filter cannot correct. The classifier must reject this tree entirely.
    classify(Not(EqualTo("msg", "hello world"))) shouldBe false
  }

  test("Not(EqualNullSafe) on a text_and_string field is NOT a candidate") {
    classify(Not(EqualNullSafe("msg", "x"))) shouldBe false
  }

  test("Not(In) on a text_and_string field is NOT a candidate") {
    classify(Not(In("msg", Array[Any]("a", "b")))) shouldBe false
  }

  test("Not of a non-candidate leaf is NOT a candidate") {
    // Not of a non-candidate is trivially not a candidate — no leaf eligibility, and the Not also
    // disqualifies the tree. Both conditions fail.
    classify(Not(EqualTo("other", "x"))) shouldBe false
  }

  // ═══════════════════════════════════════════════════════════════════
  //  And / Or — positive compound cases
  // ═══════════════════════════════════════════════════════════════════

  test("And(candidate, supported-leaf) is a candidate") {
    // And(EqualTo(msg, 'x'), EqualTo(other, 'y')): both are supersets of the intended predicate;
    // their intersection is a superset of the exact result. Spark's post-filter narrows it.
    classify(And(EqualTo("msg", "x"), EqualTo("other", "y"))) shouldBe true
  }

  test("And(candidate, candidate) is a candidate") {
    classify(And(EqualTo("msg", "x"), In("msg", Array[Any]("a")))) shouldBe true
  }

  test("Or(candidate, non-candidate) is a candidate") {
    classify(Or(EqualTo("msg", "x"), EqualTo("other", "y"))) shouldBe true
  }

  test("Or(candidate, candidate) is a candidate") {
    classify(Or(EqualTo("msg", "x"), EqualTo("msg", "y"))) shouldBe true
  }

  // ═══════════════════════════════════════════════════════════════════
  //  And / Or — negative compound cases (no candidate leaf)
  // ═══════════════════════════════════════════════════════════════════

  test("And of two non-candidates is NOT a candidate") {
    classify(And(EqualTo("other", "x"), EqualTo("other", "y"))) shouldBe false
  }

  test("Or of two non-candidates is NOT a candidate") {
    classify(Or(EqualTo("other", "x"), EqualTo("other", "y"))) shouldBe false
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Critical: Not nested inside And/Or trees containing a candidate
  // ═══════════════════════════════════════════════════════════════════

  test("And(Not(candidate), other) is NOT a candidate") {
    // If this tree were classified as a candidate, the converter would build
    // MUST(termQuery(other)) + MUST(MATCH_ALL + MUST_NOT(phrase)) → a subset of the true result
    // that Spark cannot correct. The classifier must reject it.
    classify(And(Not(EqualTo("msg", "x")), EqualTo("other", "y"))) shouldBe false
  }

  test("Or(Not(candidate), other) is NOT a candidate") {
    classify(Or(Not(EqualTo("msg", "x")), EqualTo("other", "y"))) shouldBe false
  }

  test("Not(And(candidate, other)) is NOT a candidate") {
    classify(Not(And(EqualTo("msg", "x"), EqualTo("other", "y")))) shouldBe false
  }

  test("Not(Or(candidate, candidate)) is NOT a candidate") {
    classify(Not(Or(EqualTo("msg", "x"), EqualTo("msg", "y")))) shouldBe false
  }

  test("Deeply nested tree with a buried Not over a candidate is NOT a candidate") {
    // And(Or(candidate, Not(candidate)), candidate)
    val tree = And(
      Or(EqualTo("msg", "a"), Not(EqualTo("msg", "b"))),
      EqualTo("msg", "c")
    )
    classify(tree) shouldBe false
  }

  test("Deeply nested tree with a Not over a non-candidate still rejects if any candidate is present") {
    // And(EqualTo(msg, a), Not(EqualTo(other, b))): the Not is on a non-candidate, but the
    // isNotFree check rejects ANY Not in the tree. This is conservative — the Not over "other"
    // is actually safe because "other" uses exact pushdown — but the classifier errs on the
    // side of safety. This test locks in the conservative behavior so a future optimizer change
    // that tries to be clever here must consciously opt in.
    classify(And(EqualTo("msg", "a"), Not(EqualTo("other", "b")))) shouldBe false
  }

  test("Or(IsNull, Not(EqualTo(candidate))) — Catalyst's nullable-rewrite shape — is NOT a candidate") {
    // This is what Catalyst produces from `col("msg") =!= "foo bar"` on a nullable String column.
    // The whole tree must be rejected because isNotFree(Or(...)) hits the Not branch and returns false.
    classify(Or(IsNull("msg"), Not(EqualTo("msg", "foo bar")))) shouldBe false
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Field-name case sensitivity
  // ═══════════════════════════════════════════════════════════════════

  test("field name casing flows through the injected predicate") {
    // The helper uses `_.toLowerCase == "msg"`, so uppercase field names still resolve.
    classify(EqualTo("MSG", "x")) shouldBe true
    classify(EqualTo("Msg", "x")) shouldBe true
  }

  // ═══════════════════════════════════════════════════════════════════
  //  Unsupported leaf types at the root
  // ═══════════════════════════════════════════════════════════════════

  test("StringContains on a candidate field is NOT a candidate (not a leaf EqualTo/In)") {
    classify(StringContains("msg", "sub")) shouldBe false
  }

  test("StringEndsWith on a candidate field is NOT a candidate") {
    classify(StringEndsWith("msg", "suf")) shouldBe false
  }
}
