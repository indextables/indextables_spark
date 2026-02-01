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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip_addr", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "client_ip,server_ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        ("server3", "::1"),                              // Loopback
        ("server4", "fe80::1"),                          // Link-local
        ("server5", "2001:db8:85a3::8a2e:370:7334")      // Full IPv6
      ).toDF("name", "ip")

      data.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test SUM with IP filter
      val sumResult = df.filter($"ip" === "192.168.1.1").agg(functions.sum("requests"))
      sumResult.collect()(0).getLong(0) shouldBe 300

      // Test AVG with IP filter
      val avgResult = df.filter($"ip" === "192.168.1.1").agg(functions.avg("latency"))
      avgResult.collect()(0).getDouble(0) shouldBe 15.5 +- 0.01

      // Test COUNT(*) with IP range filter
      val countResult = df.filter($"ip" >= "192.168.1.0" && $"ip" <= "192.168.1.255")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .load(tablePath)

      // Test GROUP BY on IP field with COUNT
      val groupByResult = df.groupBy("ip").count().orderBy($"count".desc)
      val results = groupByResult.collect()

      results.length shouldBe 3
      // 192.168.1.1 and 192.168.1.2 both have 2 entries
      results(0).getLong(1) shouldBe 2
      results(1).getLong(1) shouldBe 2
      results(2).getLong(1) shouldBe 1 // 10.0.0.1 has 1 entry

      // Test GROUP BY with SUM
      val sumByIp = df.groupBy("ip").agg(functions.sum("requests").as("total_requests"))
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.ip", "ip")
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
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
}
