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

package io.indextables.spark.util

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class JsonUtilTest extends AnyFunSuite with Matchers {

  test("string array round-trip preserves comma-bearing names") {
    // BUG5: the legacy CSV serializer split column names on ',', corrupting
    // names like 'revenue,usd'. The JSON path must preserve them verbatim.
    val original = Seq("revenue,usd", "name", "tags,csv,style")
    val json = JsonUtil.toJson(original)
    val parsed = JsonUtil.parseStringArray(json)
    parsed should contain theSameElementsInOrderAs original
  }

  test("string array round-trip preserves single-element with comma") {
    val original = Seq("a,b")
    val json = JsonUtil.toJson(original)
    JsonUtil.parseStringArray(json) should contain theSameElementsInOrderAs original
  }

  test("string array round-trip preserves empty list") {
    val original = Seq.empty[String]
    val json = JsonUtil.toJson(original)
    JsonUtil.parseStringArray(json) shouldBe empty
  }

  test("string array round-trip preserves names with quotes and special chars") {
    val original = Seq("col\"with\"quotes", "col with spaces", "col\\with\\backslash")
    val json = JsonUtil.toJson(original)
    JsonUtil.parseStringArray(json) should contain theSameElementsInOrderAs original
  }
}
