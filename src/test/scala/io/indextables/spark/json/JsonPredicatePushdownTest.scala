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

package io.indextables.spark.json

import org.apache.spark.sql.sources._
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import io.indextables.spark.core.IndexTables4SparkOptions
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

class JsonPredicatePushdownTest extends AnyFunSuite with Matchers {

  private def createOptions(configMap: Map[String, String] = Map.empty): IndexTables4SparkOptions = {
    new IndexTables4SparkOptions(new CaseInsensitiveStringMap(configMap.asJava))
  }

  private val structSchema = StructType(Seq(
    StructField("id", StringType),
    StructField("user", StructType(Seq(
      StructField("name", StringType),
      StructField("age", IntegerType),
      StructField("city", StringType)
    )))
  ))

  private val arraySchema = StructType(Seq(
    StructField("id", StringType),
    StructField("tags", ArrayType(StringType))
  ))

  private val nestedArraySchema = StructType(Seq(
    StructField("id", StringType),
    StructField("reviews", ArrayType(StructType(Seq(
      StructField("rating", IntegerType),
      StructField("comment", StringType)
    ))))
  ))

  test("canPushDown detects nested equality filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = EqualTo("user.name", "Alice")

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown detects nested range filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    translator.canPushDown(GreaterThan("user.age", 25)) shouldBe true
    translator.canPushDown(GreaterThanOrEqual("user.age", 25)) shouldBe true
    translator.canPushDown(LessThan("user.age", 35)) shouldBe true
    translator.canPushDown(LessThanOrEqual("user.age", 35)) shouldBe true
  }

  test("canPushDown detects IsNotNull filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    translator.canPushDown(IsNotNull("user.name")) shouldBe true
  }

  test("canPushDown detects IsNull filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    translator.canPushDown(IsNull("user.name")) shouldBe true
  }

  test("canPushDown detects And filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = And(
      EqualTo("user.name", "Alice"),
      GreaterThan("user.age", 25)
    )

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown detects Or filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Or(
      EqualTo("user.name", "Alice"),
      EqualTo("user.name", "Bob")
    )

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown detects Not filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Not(EqualTo("user.name", "Alice"))

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown rejects non-nested filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = EqualTo("id", "123")

    translator.canPushDown(filter) shouldBe false
  }

  test("canPushDown rejects unsupported filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = StringStartsWith("user.name", "Ali")

    translator.canPushDown(filter) shouldBe false
  }

  test("canPushDown detects array contains filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(arraySchema, mapper)

    val filter = StringContains("tags", "laptop")

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown detects nested array field filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(nestedArraySchema, mapper)

    val filter = GreaterThan("reviews.rating", 4)

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown handles complex boolean combinations") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Or(
      And(
        EqualTo("user.city", "NYC"),
        GreaterThan("user.age", 25)
      ),
      EqualTo("user.name", "Alice")
    )

    translator.canPushDown(filter) shouldBe true
  }

  test("canPushDown rejects mixed supported and unsupported filters") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = And(
      EqualTo("user.name", "Alice"),
      StringStartsWith("user.city", "New")  // Not supported
    )

    // And requires both sides to be pushable
    translator.canPushDown(filter) shouldBe false
  }

  test("splitNestedAttribute correctly splits simple nested path") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    // Use reflection to access private method for testing
    val method = translator.getClass.getDeclaredMethod("splitNestedAttribute", classOf[String])
    method.setAccessible(true)
    val result = method.invoke(translator, "user.name").asInstanceOf[(String, String)]

    result shouldBe ("user", "name")
  }

  test("splitNestedAttribute correctly splits multi-level nested path") {
    val deepSchema = StructType(Seq(
      StructField("user", StructType(Seq(
        StructField("address", StructType(Seq(
          StructField("city", StringType)
        )))
      )))
    ))

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(deepSchema, mapper)

    val method = translator.getClass.getDeclaredMethod("splitNestedAttribute", classOf[String])
    method.setAccessible(true)
    val result = method.invoke(translator, "user.address.city").asInstanceOf[(String, String)]

    result shouldBe ("user", "address.city")
  }

  test("isNestedAttribute detects nested struct field") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val method = translator.getClass.getDeclaredMethod("isNestedAttribute", classOf[String])
    method.setAccessible(true)

    method.invoke(translator, "user.name").asInstanceOf[Boolean] shouldBe true
  }

  test("isNestedAttribute detects nested array field") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(nestedArraySchema, mapper)

    val method = translator.getClass.getDeclaredMethod("isNestedAttribute", classOf[String])
    method.setAccessible(true)

    method.invoke(translator, "reviews.rating").asInstanceOf[Boolean] shouldBe true
  }

  test("isNestedAttribute returns false for non-nested field") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val method = translator.getClass.getDeclaredMethod("isNestedAttribute", classOf[String])
    method.setAccessible(true)

    method.invoke(translator, "id").asInstanceOf[Boolean] shouldBe false
  }

  test("isArrayField detects array field") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(arraySchema, mapper)

    val method = translator.getClass.getDeclaredMethod("isArrayField", classOf[String])
    method.setAccessible(true)

    method.invoke(translator, "tags").asInstanceOf[Boolean] shouldBe true
  }

  test("isArrayField returns false for non-array field") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val method = translator.getClass.getDeclaredMethod("isArrayField", classOf[String])
    method.setAccessible(true)

    method.invoke(translator, "user").asInstanceOf[Boolean] shouldBe false
  }

  // ========== parseQuery String Generation Tests ==========

  test("translateFilterToParseQuery generates correct term query") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = EqualTo("user.name", "Alice")
    val result = translator.translateFilterToParseQuery(filter)

    result shouldBe Some("""user.name:"Alice"""")
  }

  test("translateFilterToParseQuery generates correct range queries") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    translator.translateFilterToParseQuery(GreaterThan("user.age", 25)) shouldBe Some("user.age:>25")
    translator.translateFilterToParseQuery(GreaterThanOrEqual("user.age", 25)) shouldBe Some("user.age:>=25")
    translator.translateFilterToParseQuery(LessThan("user.age", 35)) shouldBe Some("user.age:<35")
    translator.translateFilterToParseQuery(LessThanOrEqual("user.age", 35)) shouldBe Some("user.age:<=35")
  }

  test("translateFilterToParseQuery generates correct existence queries") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    translator.translateFilterToParseQuery(IsNotNull("user.name")) shouldBe Some("user.name:*")
    translator.translateFilterToParseQuery(IsNull("user.name")) shouldBe Some("NOT user.name:*")
  }

  test("translateFilterToParseQuery generates correct AND query") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = And(
      EqualTo("user.name", "Alice"),
      GreaterThan("user.age", 25)
    )

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""(user.name:"Alice") AND (user.age:>25)""")
  }

  test("translateFilterToParseQuery generates correct OR query") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Or(
      EqualTo("user.name", "Alice"),
      EqualTo("user.name", "Bob")
    )

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""(user.name:"Alice") OR (user.name:"Bob")""")
  }

  test("translateFilterToParseQuery generates correct NOT query") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Not(EqualTo("user.name", "Alice"))

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""NOT (user.name:"Alice")""")
  }

  test("translateFilterToParseQuery handles complex boolean combinations") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Or(
      And(
        EqualTo("user.city", "NYC"),
        GreaterThan("user.age", 25)
      ),
      EqualTo("user.name", "Alice")
    )

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""((user.city:"NYC") AND (user.age:>25)) OR (user.name:"Alice")""")
  }

  test("translateFilterToParseQuery handles array contains filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(arraySchema, mapper)

    val filter = StringContains("tags", "laptop")

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""tags:"laptop"""")
  }

  test("translateFilterToParseQuery escapes special characters in values") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    // Test escaping quotes
    val filter1 = EqualTo("user.name", """Alice "The Great"""")
    val result1 = translator.translateFilterToParseQuery(filter1)
    result1 shouldBe Some("""user.name:"Alice \"The Great\""""")

    // Test escaping backslashes
    val filter2 = EqualTo("user.name", """Alice\Bob""")
    val result2 = translator.translateFilterToParseQuery(filter2)
    result2 shouldBe Some("""user.name:"Alice\\Bob"""")
  }

  test("translateFilterToParseQuery returns None for non-nested filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = EqualTo("id", "123")

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe None
  }

  test("translateFilterToParseQuery returns None for unsupported filter") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = StringStartsWith("user.name", "Ali")

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe None
  }

  test("translateFilterToParseQuery handles deep nested paths") {
    val deepSchema = StructType(Seq(
      StructField("profile", StructType(Seq(
        StructField("user", StructType(Seq(
          StructField("address", StructType(Seq(
            StructField("city", StringType)
          )))
        )))
      )))
    ))

    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(deepSchema, mapper)

    val filter = EqualTo("profile.user.address.city", "NYC")

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe Some("""profile.user.address.city:"NYC"""")
  }

  test("translateFilterToParseQuery handles numeric values in range queries") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    // Integer
    translator.translateFilterToParseQuery(GreaterThan("user.age", 30)) shouldBe Some("user.age:>30")

    // Double
    val schemaWithDouble = StructType(Seq(
      StructField("metrics", StructType(Seq(
        StructField("score", DoubleType)
      )))
    ))
    val translator2 = new JsonPredicateTranslator(schemaWithDouble, mapper)
    translator2.translateFilterToParseQuery(GreaterThan("metrics.score", 95.5)) shouldBe Some("metrics.score:>95.5")
  }

  test("translateFilterToParseQuery returns None when And child fails") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = And(
      EqualTo("user.name", "Alice"),
      StringStartsWith("user.city", "New")  // Not supported
    )

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe None
  }

  test("translateFilterToParseQuery returns None when Or child fails") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Or(
      EqualTo("user.name", "Alice"),
      StringStartsWith("user.city", "New")  // Not supported
    )

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe None
  }

  test("translateFilterToParseQuery returns None when Not child fails") {
    val mapper = new SparkSchemaToTantivyMapper(createOptions())
    val translator = new JsonPredicateTranslator(structSchema, mapper)

    val filter = Not(StringStartsWith("user.name", "Ali"))  // Not supported

    val result = translator.translateFilterToParseQuery(filter)
    result shouldBe None
  }
}
