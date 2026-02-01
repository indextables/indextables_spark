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

package io.indextables.spark.indexing

import io.indextables.spark.TestBase

/**
 * Tests for IP address field type with IndexQuery (native Tantivy query syntax).
 *
 * IndexQuery uses Tantivy's query parser syntax for IP addresses:
 *   - Equality: ip_field:192.168.1.1
 *   - Range: ip_field:[192.168.1.1 TO 192.168.1.10]
 *   - Multiple terms (IN): ip_field:192.168.1.1 OR ip_field:10.0.0.1
 *
 * Note: Wildcard queries (like 192.168.1.*) are NOT supported on IP address fields.
 * Use range queries for subnet-style matching.
 */
class IpAddressIndexQueryTest extends TestBase {

  private var testDataPath: String = _
  private var sharedTempDir: java.nio.file.Path = _

  override def beforeAll(): Unit = {
    super.beforeAll()

    val spark = this.spark
    import spark.implicits._

    // Create a dedicated temp directory for this test suite
    sharedTempDir = java.nio.file.Files.createTempDirectory("ip-indexquery-test")
    testDataPath = s"${sharedTempDir.toString}/ip_indexquery_test"

    // Create test data with IP addresses
    val testData = Seq(
      (1, "server1", "192.168.1.1", "us-east", 100),
      (2, "server2", "192.168.1.2", "us-east", 200),
      (3, "server3", "192.168.1.3", "us-west", 150),
      (4, "server4", "192.168.1.10", "us-west", 250),
      (5, "server5", "192.168.1.100", "eu-west", 300),
      (6, "server6", "10.0.0.1", "eu-west", 50),
      (7, "server7", "10.0.0.2", "ap-south", 75),
      (8, "server8", "2001:db8::1", "ap-south", 125),  // IPv6
      (9, "server9", "2001:db8::2", "us-east", 175),   // IPv6
      (10, "server10", "172.16.0.1", "us-west", 225)
    ).toDF("id", "name", "ip", "region", "requests")

    // Write with IP field configuration
    testData.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.ip", "ip")
      .mode("overwrite")
      .save(testDataPath)

    println(s"Created IP address IndexQuery test dataset at $testDataPath")
  }

  override def afterAll(): Unit = {
    if (sharedTempDir != null) {
      deleteRecursively(sharedTempDir.toFile)
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()

    // Register temp view for SQL queries
    val df = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testDataPath)

    df.createOrReplaceTempView("ip_servers")
  }

  test("IP address IndexQuery - exact match (equality)") {
    val spark = this.spark

    // Test exact IP match using IndexQuery syntax
    val result = spark.sql("SELECT * FROM ip_servers WHERE ip indexquery '192.168.1.1'").collect()

    result.length shouldBe 1
    result(0).getAs[String]("name") shouldBe "server1"
  }

  test("IP address IndexQuery - multiple exact matches (IN equivalent)") {
    val spark = this.spark

    // Test multiple IPs using OR in IndexQuery
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '192.168.1.1 OR 10.0.0.1'"
    ).collect()

    result.length shouldBe 2
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server1", "server6")
  }

  test("IP address IndexQuery - range query (inclusive)") {
    val spark = this.spark

    // Test IP range using Tantivy range syntax [inclusive TO inclusive]
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '[192.168.1.1 TO 192.168.1.3]'"
    ).collect()

    result.length shouldBe 3
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server1", "server2", "server3")
  }

  test("IP address IndexQuery - range query (exclusive)") {
    val spark = this.spark

    // Test IP range using Tantivy range syntax {exclusive TO exclusive}
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '{192.168.1.1 TO 192.168.1.3}'"
    ).collect()

    // Only 192.168.1.2 should match (1 and 3 are excluded)
    result.length shouldBe 1
    result(0).getAs[String]("name") shouldBe "server2"
  }

  test("IP address IndexQuery - range query (mixed inclusive/exclusive)") {
    val spark = this.spark

    // Test IP range [inclusive TO exclusive}
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '[192.168.1.1 TO 192.168.1.3}'"
    ).collect()

    // 192.168.1.1 and 192.168.1.2 should match (3 is excluded)
    result.length shouldBe 2
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server1", "server2")
  }

  test("IP address IndexQuery - IPv6 exact match") {
    val spark = this.spark

    // Test IPv6 address matching - need to quote due to colons
    val result = spark.sql(
      """SELECT * FROM ip_servers WHERE ip indexquery '"2001:db8::1"'"""
    ).collect()

    result.length shouldBe 1
    result(0).getAs[String]("name") shouldBe "server8"
  }

  test("IP address IndexQuery - IPv6 multiple exact matches") {
    val spark = this.spark

    // Test IPv6 multiple addresses using OR
    // Note: IPv6 range queries via IndexQuery are not supported due to Tantivy parser limitations
    // Use Spark filter syntax ($"ip".between(...)) for IPv6 ranges instead
    val result = spark.sql(
      """SELECT * FROM ip_servers WHERE ip indexquery '"2001:db8::1" OR "2001:db8::2"'"""
    ).collect()

    result.length shouldBe 2
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server8", "server9")
  }

  test("IP address IndexQuery - combined with regular filters") {
    val spark = this.spark

    // Test IndexQuery combined with regular SQL filters
    // Use range query to match 192.168.1.x subnet
    val result = spark.sql(
      """SELECT * FROM ip_servers
         WHERE ip indexquery '[192.168.1.0 TO 192.168.1.255]'
         AND region = 'us-east'"""
    ).collect()

    // 192.168.1.1 and 192.168.1.2 are in us-east
    result.length shouldBe 2
    result.foreach { row =>
      row.getAs[String]("region") shouldBe "us-east"
    }
  }

  test("IP address IndexQuery - with aggregation") {
    val spark = this.spark

    // Test IndexQuery with aggregation using range for subnet
    val result = spark.sql(
      """SELECT COUNT(*) as cnt, SUM(requests) as total_requests
         FROM ip_servers
         WHERE ip indexquery '[192.168.1.0 TO 192.168.1.255]'"""
    ).collect()

    result.length shouldBe 1
    result(0).getAs[Long]("cnt") shouldBe 5
    // Sum: 100 + 200 + 150 + 250 + 300 = 1000
    result(0).getAs[Long]("total_requests") shouldBe 1000
  }

  test("IP address IndexQuery - 10.x.x.x subnet via range") {
    val spark = this.spark

    // Test matching 10.0.0.x addresses using range
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '[10.0.0.0 TO 10.0.0.255]'"
    ).collect()

    result.length shouldBe 2
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server6", "server7")
  }

  test("IP address IndexQuery - multiple ranges with OR") {
    val spark = this.spark

    // Test matching multiple subnets using OR with ranges
    val result = spark.sql(
      """SELECT * FROM ip_servers
         WHERE ip indexquery '[10.0.0.0 TO 10.0.0.255] OR [172.16.0.0 TO 172.16.0.255]'"""
    ).collect()

    result.length shouldBe 3
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server6", "server7", "server10")
  }

  test("IP address IndexQuery - NOT query (exclusion) using AND NOT") {
    val spark = this.spark

    // Test NOT query - match range but exclude specific IP
    val result = spark.sql(
      """SELECT * FROM ip_servers
         WHERE ip indexquery '[192.168.1.1 TO 192.168.1.3] AND NOT 192.168.1.1'"""
    ).collect()

    // Should match 192.168.1.2, 192.168.1.3 (excluding 192.168.1.1)
    result.length shouldBe 2
    val names = result.map(_.getAs[String]("name")).toSet
    names shouldBe Set("server2", "server3")
  }

  test("IP address IndexQuery - greater than using open range") {
    val spark = this.spark

    // Test greater than using open-ended range {value TO *]
    // In Tantivy IP ordering, this matches IPs > 192.168.1.3
    // which includes 192.168.1.10, 192.168.1.100, and IPv6 addresses
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '{192.168.1.3 TO *]'"
    ).collect()

    // Should match IPs > 192.168.1.3 in Tantivy's IP ordering
    // This includes: 192.168.1.10, 192.168.1.100, and IPv6 addresses (2001:db8::1, 2001:db8::2)
    result.length should be >= 2  // At least the 192.168.1.x addresses
    val names = result.map(_.getAs[String]("name")).toSet
    names should contain("server4")  // 192.168.1.10
    names should contain("server5")  // 192.168.1.100
  }

  test("IP address IndexQuery - less than using open range") {
    val spark = this.spark

    // Test less than using open-ended range [* TO value}
    val result = spark.sql(
      "SELECT * FROM ip_servers WHERE ip indexquery '[* TO 192.168.1.2}'"
    ).collect()

    // Should match IPs < 192.168.1.2 in Tantivy's IP ordering
    // Note: Tantivy may normalize IPs to IPv6-mapped format (::ffff:10.0.0.1)
    result.length should be >= 1
    val names = result.map(_.getAs[String]("name")).toSet
    names should contain("server1")  // 192.168.1.1
  }
}
