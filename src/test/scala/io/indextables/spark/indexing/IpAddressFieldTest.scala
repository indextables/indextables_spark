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

import org.apache.spark.sql.functions

import io.indextables.spark.TestBase

/**
 * Tests for IP address field type support.
 *
 * IP address fields support:
 *   - Term queries (exact match): ip === "192.168.1.1"
 *   - Range queries: ip >= "192.168.1.0" && ip <= "192.168.1.255"
 *   - IN queries: ip.isin("192.168.1.1", "10.0.0.1")
 *   - Both IPv4 and IPv6 addresses
 */
class IpAddressFieldTest extends TestBase {

  test("IP address field configuration and exact matching") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1"),
        ("server2", "192.168.1.2"),
        ("server3", "10.0.0.1"),
        ("server4", "2001:db8::1") // IPv6
      ).toDF("name", "ip_addr")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip_addr", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Verify data was written
      df.count() shouldBe 4

      // Test exact match for IPv4
      val exactMatchResult = df.filter($"ip_addr" === "192.168.1.1")
      exactMatchResult.count() shouldBe 1
      exactMatchResult.collect()(0).getString(0) shouldBe "server1"

      // Test exact match for another IPv4
      df.filter($"ip_addr" === "10.0.0.1").count() shouldBe 1

      // Test exact match for IPv6
      df.filter($"ip_addr" === "2001:db8::1").count() shouldBe 1
    }
  }

  test("IP address range queries") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      // Create data with sequential IP addresses
      val data = (1 to 10).map(i => (s"server$i", s"192.168.1.$i")).toDF("name", "ip")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test range query: >= and <=
      val rangeResult = df.filter($"ip" >= "192.168.1.3" && $"ip" <= "192.168.1.7")
      rangeResult.count() shouldBe 5

      // Test greater than
      df.filter($"ip" > "192.168.1.8").count() shouldBe 2 // .9 and .10

      // Test less than
      df.filter($"ip" < "192.168.1.3").count() shouldBe 2 // .1 and .2

      // Test greater than or equal
      df.filter($"ip" >= "192.168.1.9").count() shouldBe 2 // .9 and .10

      // Test less than or equal
      df.filter($"ip" <= "192.168.1.2").count() shouldBe 2 // .1 and .2

      // Test BETWEEN operator (equivalent to >= AND <=)
      df.filter($"ip".between("192.168.1.3", "192.168.1.7")).count() shouldBe 5
    }
  }

  test("IP address IN query") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1"),
        ("server2", "192.168.1.2"),
        ("server3", "10.0.0.1"),
        ("server4", "10.0.0.2")
      ).toDF("name", "ip")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test IN query with multiple IPs
      val inResult = df.filter($"ip".isin("192.168.1.1", "10.0.0.1"))
      inResult.count() shouldBe 2

      // Verify the correct servers are returned
      val names = inResult.select("name").collect().map(_.getString(0)).toSet
      names shouldBe Set("server1", "server3")
    }
  }

  test("IP address list-based typemap syntax") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("req1", "192.168.1.1", "10.0.0.1"),
        ("req2", "192.168.1.2", "10.0.0.2")
      ).toDF("request_id", "client_ip", "server_ip")

      // Use list-based syntax for multiple IP fields
      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "client_ip,server_ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test filtering on client_ip
      df.filter($"client_ip" === "192.168.1.1").count() shouldBe 1

      // Test filtering on server_ip
      df.filter($"server_ip" === "10.0.0.1").count() shouldBe 1

      // Test combined filter
      df.filter($"client_ip" === "192.168.1.1" && $"server_ip" === "10.0.0.1").count() shouldBe 1
    }
  }

  test("IP address mixed with other field types") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        (1, "web", "192.168.1.1", 8080),
        (2, "api", "192.168.1.2", 443),
        (3, "db", "10.0.0.1", 5432)
      ).toDF("id", "service", "ip", "port")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test combined filter with IP and other fields
      df.filter($"ip" === "192.168.1.1" && $"port" === 8080).count() shouldBe 1
      df.filter($"service" === "api" && $"ip" === "192.168.1.2").count() shouldBe 1
    }
  }

  test("IPv6 address support") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "2001:db8::1"),
        ("server2", "2001:db8::2"),
        ("server3", "::1"),                         // Loopback
        ("server4", "fe80::1"),                     // Link-local
        ("server5", "2001:db8:85a3::8a2e:370:7334") // Full IPv6
      ).toDF("name", "ip")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test exact match for various IPv6 formats
      df.filter($"ip" === "2001:db8::1").count() shouldBe 1
      df.filter($"ip" === "::1").count() shouldBe 1
      df.filter($"ip" === "fe80::1").count() shouldBe 1
      df.filter($"ip" === "2001:db8:85a3::8a2e:370:7334").count() shouldBe 1

      // Test IN query with IPv6
      df.filter($"ip".isin("2001:db8::1", "::1")).count() shouldBe 2
    }
  }

  test("IP address not equal filter") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1"),
        ("server2", "192.168.1.2"),
        ("server3", "192.168.1.3")
      ).toDF("name", "ip")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test not equal - should return 2 servers
      val notEqualResult = df.filter($"ip" =!= "192.168.1.1")
      notEqualResult.count() shouldBe 2

      val names = notEqualResult.select("name").collect().map(_.getString(0)).toSet
      names shouldBe Set("server2", "server3")
    }
  }

  test("IP address simple aggregate (COUNT with filter)") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1"),
        ("server2", "192.168.1.1"), // Duplicate IP
        ("server3", "192.168.1.2"),
        ("server4", "10.0.0.1")
      ).toDF("name", "ip")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test count with IP filter (simple aggregate pushdown)
      df.filter($"ip" === "192.168.1.1").count() shouldBe 2

      // Test total count (simple aggregate)
      df.count() shouldBe 4

      // Test count with range filter
      df.filter($"ip" >= "192.168.1.1" && $"ip" <= "192.168.1.2").count() shouldBe 3
    }
  }

  test("IP address simple aggregate with numeric fields (COUNT, SUM, AVG)") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1", 100, 10.5),
        ("server2", "192.168.1.1", 200, 20.5),
        ("server3", "192.168.1.2", 150, 15.0),
        ("server4", "10.0.0.1", 50, 5.0)
      ).toDF("name", "ip", "requests", "latency")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test SUM with IP filter
      val sumResult = df.filter($"ip" === "192.168.1.1").agg(functions.sum("requests"))
      sumResult.collect()(0).getLong(0) shouldBe 300

      // Test AVG with IP filter
      val avgResult = df.filter($"ip" === "192.168.1.1").agg(functions.avg("latency"))
      avgResult.collect()(0).getDouble(0) shouldBe 15.5 +- 0.01

      // Test COUNT(*) with IP range filter
      val countResult = df
        .filter($"ip" >= "192.168.1.0" && $"ip" <= "192.168.1.255")
        .agg(functions.count("*"))
      countResult.collect()(0).getLong(0) shouldBe 3
    }
  }

  test("IP address group-by aggregate") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1", 100),
        ("server2", "192.168.1.1", 200),
        ("server3", "192.168.1.2", 150),
        ("server4", "192.168.1.2", 250),
        ("server5", "10.0.0.1", 50)
      ).toDF("name", "ip", "requests")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test GROUP BY on IP field with COUNT
      val groupByResult = df.groupBy("ip").count().orderBy($"count".desc)
      val results       = groupByResult.collect()

      results.length shouldBe 3
      // 192.168.1.1 and 192.168.1.2 both have 2 entries
      results(0).getLong(1) shouldBe 2
      results(1).getLong(1) shouldBe 2
      results(2).getLong(1) shouldBe 1 // 10.0.0.1 has 1 entry

      // Test GROUP BY with SUM
      val sumByIp = df
        .groupBy("ip")
        .agg(functions.sum("requests").as("total_requests"))
        .orderBy($"total_requests".desc)
      val sumResults = sumByIp.collect()

      sumResults.length shouldBe 3
      // Find 192.168.1.2: 150 + 250 = 400
      val ip2Result = sumResults.find(_.getString(0) == "192.168.1.2")
      ip2Result.isDefined shouldBe true
      ip2Result.get.getLong(1) shouldBe 400

      // Find 192.168.1.1: 100 + 200 = 300
      val ip1Result = sumResults.find(_.getString(0) == "192.168.1.1")
      ip1Result.isDefined shouldBe true
      ip1Result.get.getLong(1) shouldBe 300
    }
  }

  test("IP address group-by aggregate with filter") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1", "web", 100),
        ("server2", "192.168.1.1", "api", 200),
        ("server3", "192.168.1.2", "web", 150),
        ("server4", "10.0.0.1", "db", 50)
      ).toDF("name", "ip", "service", "requests")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test GROUP BY with filter on IP range
      val filteredGroupBy = df
        .filter($"ip" >= "192.168.1.0" && $"ip" <= "192.168.1.255")
        .groupBy("ip")
        .count()
        .orderBy("ip")

      val results = filteredGroupBy.collect()
      results.length shouldBe 2 // Only 192.168.1.1 and 192.168.1.2

      // Test GROUP BY service with IP filter
      val serviceGroupBy = df
        .filter($"ip" === "192.168.1.1")
        .groupBy("service")
        .count()

      val serviceResults = serviceGroupBy.collect()
      serviceResults.length shouldBe 2 // web and api
    }
  }

  test("IP address CIDR /24 equality filter") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      // IPs at and around 192.168.1.0/24 boundaries
      val data = Seq(
        ("outside-low",  "192.168.0.255"),
        ("boundary-low", "192.168.1.0"),
        ("inside1",      "192.168.1.1"),
        ("inside2",      "192.168.1.128"),
        ("boundary-hi",  "192.168.1.255"),
        ("outside-hi",   "192.168.2.0"),
        ("unrelated",    "10.0.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // CIDR notation transparently expands to range [192.168.1.0, 192.168.1.255]
      val cidrResult = df.filter($"ip" === "192.168.1.0/24").collect()
      cidrResult.length shouldBe 4 // boundary-low, inside1, inside2, boundary-hi

      val names = cidrResult.map(_.getString(0)).toSet
      names should contain("boundary-low")
      names should contain("inside1")
      names should contain("inside2")
      names should contain("boundary-hi")
      names should not contain "outside-low"
      names should not contain "outside-hi"
      names should not contain "unrelated"
    }
  }

  test("IP address CIDR /8 equality filter") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("ten-a",    "10.0.0.1"),
        ("ten-b",    "10.255.255.254"),
        ("other",    "172.16.0.1"),
        ("other2",   "192.168.1.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val cidrResult = df.filter($"ip" === "10.0.0.0/8").collect()
      cidrResult.length shouldBe 2
      cidrResult.map(_.getString(0)).toSet shouldBe Set("ten-a", "ten-b")
    }
  }

  test("IP address CIDR /32 is exact match") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("s1", "192.168.1.1"),
        ("s2", "192.168.1.2"),
        ("s3", "192.168.1.3")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // /32 should be equivalent to exact term match
      val cidrResult = df.filter($"ip" === "192.168.1.2/32").collect()
      cidrResult.length shouldBe 1
      cidrResult(0).getString(0) shouldBe "s2"
    }
  }

  test("IP address wildcard last octet") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("outside-low",  "192.168.0.255"),
        ("boundary-low", "192.168.1.0"),
        ("inside1",      "192.168.1.1"),
        ("inside2",      "192.168.1.128"),
        ("boundary-hi",  "192.168.1.255"),
        ("outside-hi",   "192.168.2.0"),
        ("unrelated",    "10.0.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Wildcard expands to same range as 192.168.1.0/24
      val wildcardResult = df.filter($"ip" === "192.168.1.*").collect()
      wildcardResult.length shouldBe 4

      val names = wildcardResult.map(_.getString(0)).toSet
      names should contain("boundary-low")
      names should contain("inside1")
      names should contain("inside2")
      names should contain("boundary-hi")
    }
  }

  test("IP address wildcard multi-octet") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("ten-a",  "10.0.0.1"),
        ("ten-b",  "10.0.1.5"),
        ("other",  "172.16.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      df.filter($"ip" === "10.0.*.*").collect().length shouldBe 2
    }
  }

  test("IP address CIDR and wildcard equivalence") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = (1 to 10).map(i => (s"server$i", s"192.168.1.$i")).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val cidrIds      = df.filter($"ip" === "192.168.1.0/24").collect().map(_.getString(0)).toSet
      val wildcardIds  = df.filter($"ip" === "192.168.1.*").collect().map(_.getString(0)).toSet
      val explicitIds  = df
        .filter($"ip" >= "192.168.1.0" && $"ip" <= "192.168.1.255")
        .collect()
        .map(_.getString(0))
        .toSet

      cidrIds shouldBe explicitIds
      wildcardIds shouldBe explicitIds
    }
  }

  test("IP address CIDR in isin()") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("ten-a",     "10.0.0.1"),
        ("ten-b",     "10.0.0.2"),
        ("private-a", "192.168.1.1"),
        ("private-b", "192.168.1.2"),
        ("other",     "172.16.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // isin() with CIDR values — each is expanded to a range; results are unioned.
      val isinResult = df.filter($"ip".isin("10.0.0.0/8", "192.168.1.0/24")).collect()
      isinResult.length shouldBe 4
      isinResult.map(_.getString(0)).toSet shouldBe
        Set("ten-a", "ten-b", "private-a", "private-b")
      df.filter($"ip".isin("10.0.0.0/8", "192.168.1.0/24")).count() shouldBe 4
    }
  }

  test("IP address CIDR and literal in isin()") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("ten-a",     "10.0.0.1"),
        ("ten-b",     "10.0.0.2"),
        ("private-a", "192.168.1.1"),
        ("other",     "172.16.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // CIDR + exact IP literal in isin() — CIDR expands to range, literal is exact match.
      val result = df.filter($"ip".isin("10.0.0.0/8", "172.16.0.1")).collect()
      result.length shouldBe 3
      result.map(_.getString(0)).toSet shouldBe Set("ten-a", "ten-b", "other")
      df.filter($"ip".isin("10.0.0.0/8", "172.16.0.1")).count() shouldBe 3
    }
  }

  test("IP address wildcard in isin()") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("ten-a",     "10.0.0.1"),
        ("ten-b",     "10.0.0.2"),
        ("private-a", "192.168.1.1"),
        ("private-b", "192.168.1.2"),
        ("other",     "172.16.0.1")
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Multiple wildcards in isin() — each expanded independently.
      df.filter($"ip".isin("10.0.*.*", "192.168.1.*")).count() shouldBe 4
      val result = df.filter($"ip".isin("10.0.*.*", "192.168.1.*")).collect()
      result.map(_.getString(0)).toSet shouldBe
        Set("ten-a", "ten-b", "private-a", "private-b")

      // Mixed wildcard and literal in isin()
      df.filter($"ip".isin("10.0.*.*", "172.16.0.1")).count() shouldBe 3
      val mixedResult = df.filter($"ip".isin("10.0.*.*", "172.16.0.1")).collect()
      mixedResult.map(_.getString(0)).toSet shouldBe
        Set("ten-a", "ten-b", "other")
    }
  }

  test("IP address wildcard groupBy") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("s1", "10.0.0.1", 100),
        ("s2", "10.0.0.1", 200),
        ("s3", "10.0.1.5", 150),
        ("s4", "192.168.1.1", 50)
      ).toDF("name", "ip", "requests")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // groupBy on IP field with wildcard filter covering 10.0.0.0/8.
      val result = df
        .filter($"ip" === "10.0.*.*")
        .groupBy("ip")
        .count()
        .collect()

      // 10.0.0.1 appears twice; 10.0.1.5 appears once; 192.168.1.1 excluded
      result.length shouldBe 2
      val countByIp = result.map(r => r.getString(0) -> r.getLong(1)).toMap
      countByIp("10.0.0.1") shouldBe 2
      countByIp("10.0.1.5") shouldBe 1
    }
  }

  test("IP address CIDR aggregation") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("s1", "192.168.1.1", 100),
        ("s2", "192.168.1.2", 200),
        ("s3", "192.168.2.1", 50)
      ).toDF("name", "ip", "requests")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      val result = df
        .filter($"ip" === "192.168.1.0/24")
        .agg(functions.count("*"), functions.sum("requests"))
        .collect()(0)

      result.getLong(0) shouldBe 2
      result.getLong(1) shouldBe 300
    }
  }

  test("IP address scan with select and filter") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("server1", "192.168.1.1", "us-east", 100),
        ("server2", "192.168.1.2", "us-west", 200),
        ("server3", "10.0.0.1", "eu-west", 150),
        ("server4", "10.0.0.2", "ap-south", 250)
      ).toDF("name", "ip", "region", "requests")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      // Test scan with select and filter
      val scanResult = df
        .select("name", "ip", "region")
        .filter($"ip" === "192.168.1.1")
        .collect()

      scanResult.length shouldBe 1
      scanResult(0).getString(0) shouldBe "server1"
      scanResult(0).getString(1) shouldBe "192.168.1.1"
      scanResult(0).getString(2) shouldBe "us-east"

      // Test scan with range filter and column projection
      val rangeScanResult = df
        .select("name", "requests")
        .filter($"ip" >= "10.0.0.1" && $"ip" <= "10.0.0.2")
        .orderBy("requests")
        .collect()

      rangeScanResult.length shouldBe 2
      rangeScanResult(0).getString(0) shouldBe "server3"
      rangeScanResult(1).getString(0) shouldBe "server4"
    }
  }

  test("IP address CIDR with group-by aggregation on numeric field") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      // Two subnets, multiple IPs per subnet, each with a distinct region.
      // CIDR filter selects only the 192.168.1.0/24 subnet; group-by on region
      // exercises the GroupByAggregateColumnarReader / nativeAggregateArrowFfi path
      // to confirm rewrite_ip_term_queries runs correctly on the group-by aggregate path.
      val data = Seq(
        ("192.168.1.1", "us-east", 100),
        ("192.168.1.2", "us-east", 200),
        ("192.168.1.3", "eu-west",  50),
        ("10.0.0.1",    "us-east", 999)  // outside CIDR — must not appear in results
      ).toDF("ip", "region", "requests")

      data.write
        .format(INDEXTABLES_FORMAT)
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format(INDEXTABLES_FORMAT)
        .load(tablePath)

      val result = df
        .filter($"ip" === "192.168.1.0/24")
        .groupBy("region")
        .agg(functions.count("*").as("cnt"), functions.sum("requests").as("total"))
        .orderBy("region")
        .collect()

      // 10.0.0.1 must be excluded; only the two 192.168.1.x regions remain
      result.length shouldBe 2
      result(0).getString(0) shouldBe "eu-west"
      result(0).getLong(1)   shouldBe 1
      result(0).getLong(2)   shouldBe 50
      result(1).getString(0) shouldBe "us-east"
      result(1).getLong(1)   shouldBe 2
      result(1).getLong(2)   shouldBe 300
    }
  }

  test("IP address non-contiguous wildcard is rejected via filter pushdown") {
    withTempPath { tablePath =>
      val spark = this.spark
      import spark.implicits._

      val data = Seq(
        ("10.0.1.1", 1),
        ("10.0.2.1", 2),
        ("10.5.1.1", 3)
      ).toDF("ip", "id")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Non-contiguous wildcard 10.*.1.* cannot be represented as a single IP range.
      // All execution paths reject it with an exception (tantivy4java 0.33.3+).
      intercept[org.apache.spark.SparkException] {
        df.filter($"ip" === "10.*.1.*").collect()
      }
    }
  }
}
