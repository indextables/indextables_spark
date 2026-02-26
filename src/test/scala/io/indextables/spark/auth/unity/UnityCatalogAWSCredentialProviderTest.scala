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

package io.indextables.spark.auth.unity

import java.net.{InetSocketAddress, URI}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.ArrayBuffer

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Unit tests for UnityCatalogAWSCredentialProvider using a mock HTTP server. Does not require Spark - purely tests the
 * HTTP credential fetching logic.
 */
class UnityCatalogAWSCredentialProviderTest
    extends AnyFunSuite
    with Matchers
    with BeforeAndAfterAll
    with BeforeAndAfterEach {

  private var mockServer: HttpServer               = _
  private var serverPort: Int                      = _
  private var configMap: Map[String, String]       = _
  private val requestLog: ArrayBuffer[MockRequest] = ArrayBuffer.empty

  case class MockRequest(
    method: String,
    path: String,
    body: String,
    headers: Map[String, String])

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Start mock HTTP server on random available port
    mockServer = HttpServer.create(new InetSocketAddress(0), 0)
    serverPort = mockServer.getAddress.getPort
    mockServer.setExecutor(null)
    mockServer.start()
  }

  override def afterAll(): Unit = {
    if (mockServer != null) {
      mockServer.stop(0)
    }
    super.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    configMap = Map(
      "spark.indextables.databricks.workspaceUrl" -> s"http://localhost:$serverPort",
      "spark.indextables.databricks.apiToken"     -> "test-token-12345"
    )
    requestLog.clear()
    UnityCatalogAWSCredentialProvider.clearCache()
    // Remove any existing handlers (ignore errors if not present)
    try
      mockServer.removeContext("/api/2.1/unity-catalog/temporary-path-credentials")
    catch {
      case _: IllegalArgumentException => // Context doesn't exist, ignore
    }
  }

  override def afterEach(): Unit = {
    UnityCatalogAWSCredentialProvider.clearCache()
    super.afterEach()
  }

  private def setupMockHandler(responseCode: Int, responseBody: String): Unit = {
    // Remove existing handler first
    try
      mockServer.removeContext("/api/2.1/unity-catalog/temporary-path-credentials")
    catch {
      case _: IllegalArgumentException => // Context doesn't exist, ignore
    }

    val handler = new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod
        val path   = exchange.getRequestURI.getPath
        val body   = new String(exchange.getRequestBody.readAllBytes())
        val headers = scala.jdk.CollectionConverters
          .mapAsScalaMapConverter(exchange.getRequestHeaders)
          .asScala
          .map { case (k, v) => k -> v.get(0) }
          .toMap

        requestLog += MockRequest(method, path, body, headers)

        exchange.sendResponseHeaders(responseCode, responseBody.length)
        val os = exchange.getResponseBody
        os.write(responseBody.getBytes)
        os.close()
      }
    }
    mockServer.createContext("/api/2.1/unity-catalog/temporary-path-credentials", handler)
  }

  private def setupMockHandlerWithCallback(callback: MockRequest => (Int, String)): Unit = {
    // Remove existing handler first
    try
      mockServer.removeContext("/api/2.1/unity-catalog/temporary-path-credentials")
    catch {
      case _: IllegalArgumentException => // Context doesn't exist, ignore
    }

    val handler = new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod
        val path   = exchange.getRequestURI.getPath
        val body   = new String(exchange.getRequestBody.readAllBytes())
        val headers = scala.jdk.CollectionConverters
          .mapAsScalaMapConverter(exchange.getRequestHeaders)
          .asScala
          .map { case (k, v) => k -> v.get(0) }
          .toMap

        val request = MockRequest(method, path, body, headers)
        requestLog += request

        val (responseCode, responseBody) = callback(request)
        exchange.sendResponseHeaders(responseCode, responseBody.length)
        val os = exchange.getResponseBody
        os.write(responseBody.getBytes)
        os.close()
      }
    }
    mockServer.createContext("/api/2.1/unity-catalog/temporary-path-credentials", handler)
  }

  private def successResponse(
    accessKeyId: String = "AKIAIOSFODNN7EXAMPLE",
    secretAccessKey: String = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
    sessionToken: String = "FwoGZXIvYXdzEBY...",
    expirationTime: Long = System.currentTimeMillis() + 3600000 // 1 hour from now
  ): String =
    s"""{
       |  "aws_temp_credentials": {
       |    "access_key_id": "$accessKeyId",
       |    "secret_access_key": "$secretAccessKey",
       |    "session_token": "$sessionToken"
       |  },
       |  "expiration_time": $expirationTime
       |}""".stripMargin

  // ==================== Basic Functionality Tests ====================

  test("successfully fetches credentials from mock API") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    val credentials = provider.getCredentials()

    assert(credentials != null)
    assert(credentials.getAWSAccessKeyId == "AKIAIOSFODNN7EXAMPLE")
    assert(credentials.getAWSSecretKey == "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY")

    // Verify request details
    assert(requestLog.size == 1)
    assert(requestLog.head.method == "POST")
    assert(requestLog.head.headers.get("Authorization").contains("Bearer test-token-12345"))
    assert(requestLog.head.body.contains("PATH_READ_WRITE")) // Default operation
    assert(requestLog.head.body.contains("s3://test-bucket/path"))
  }

  test("returns session credentials when session token is provided") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    val credentials = provider.getCredentials()

    assert(credentials.isInstanceOf[com.amazonaws.auth.BasicSessionCredentials])
    val sessionCreds = credentials.asInstanceOf[com.amazonaws.auth.BasicSessionCredentials]
    assert(sessionCreds.getSessionToken == "FwoGZXIvYXdzEBY...")
  }

  // ==================== Caching Tests ====================

  test("caches credentials and does not make duplicate API calls") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    // First call - should hit API
    provider.getCredentials()
    assert(requestLog.size == 1)

    // Second call - should use cache
    provider.getCredentials()
    assert(requestLog.size == 1, "Should not make another API call due to caching")

    // Third call - still cached
    provider.getCredentials()
    assert(requestLog.size == 1)
  }

  test("different tokens get separate cache entries") {
    setupMockHandler(200, successResponse(accessKeyId = "KEY_FOR_TOKEN_A"))

    // Provider with token A
    val provider1 = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )
    val creds1 = provider1.getCredentials()
    assert(creds1.getAWSAccessKeyId == "KEY_FOR_TOKEN_A")
    assert(requestLog.size == 1)

    // Change to token B with different response
    val configMapB = configMap + ("spark.indextables.databricks.apiToken" -> "different-token-67890")
    setupMockHandler(200, successResponse(accessKeyId = "KEY_FOR_TOKEN_B"))

    val provider2 = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMapB
    )
    val creds2 = provider2.getCredentials()

    // Should have made a new API call (different token = different cache key)
    assert(requestLog.size == 2)
    assert(creds2.getAWSAccessKeyId == "KEY_FOR_TOKEN_B")
  }

  test("different paths get separate cache entries") {
    val callCount = new AtomicInteger(0)

    setupMockHandlerWithCallback { request =>
      val pathNum = callCount.incrementAndGet()
      (200, successResponse(accessKeyId = s"KEY_FOR_PATH_$pathNum"))
    }

    val provider1 = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://bucket/path1"),
      configMap
    )
    val creds1 = provider1.getCredentials()
    assert(creds1.getAWSAccessKeyId == "KEY_FOR_PATH_1")

    val provider2 = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://bucket/path2"),
      configMap
    )
    val creds2 = provider2.getCredentials()
    assert(creds2.getAWSAccessKeyId == "KEY_FOR_PATH_2")

    // Both should have triggered API calls
    assert(requestLog.size == 2)
  }

  // ==================== Expiration Tests ====================

  test("refreshes credentials when near expiration") {
    // Set short refresh buffer for testing
    val testConfig = configMap + ("spark.indextables.databricks.credential.refreshBuffer.minutes" -> "60") // 60 min buffer

    val callCount = new AtomicInteger(0)
    setupMockHandlerWithCallback { _ =>
      val num = callCount.incrementAndGet()
      // First call: expires in 30 minutes (within 60 min buffer, should trigger refresh)
      // Second call: expires in 2 hours (outside buffer)
      val expirationTime = if (num == 1) {
        System.currentTimeMillis() + (30 * 60 * 1000) // 30 min from now
      } else {
        System.currentTimeMillis() + (2 * 60 * 60 * 1000) // 2 hours from now
      }
      (200, successResponse(accessKeyId = s"KEY_$num", expirationTime = expirationTime))
    }

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      testConfig
    )

    // First call
    val creds1 = provider.getCredentials()
    assert(creds1.getAWSAccessKeyId == "KEY_1")
    assert(requestLog.size == 1)

    // Second call - should refresh because first credentials expire within buffer
    val creds2 = provider.getCredentials()
    assert(creds2.getAWSAccessKeyId == "KEY_2")
    assert(requestLog.size == 2, "Should have refreshed due to near-expiration")
  }

  // ==================== Credential Operation & Fallback Tests ====================

  test("default operation is PATH_READ_WRITE with fallback to PATH_READ on 403") {
    // Disable retries for this test to focus on fallback behavior
    val testConfig = configMap + ("spark.indextables.databricks.retry.attempts" -> "1")

    setupMockHandlerWithCallback { request =>
      if (request.body.contains("PATH_READ_WRITE")) {
        (403, """{"error": "Permission denied for READ_WRITE"}""")
      } else if (request.body.contains("PATH_READ")) {
        (200, successResponse(accessKeyId = "READ_ONLY_KEY"))
      } else {
        (400, """{"error": "Unknown operation"}""")
      }
    }

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      testConfig
    )

    val credentials = provider.getCredentials()

    assert(credentials.getAWSAccessKeyId == "READ_ONLY_KEY")
    assert(requestLog.size == 2) // First PATH_READ_WRITE (403), then PATH_READ (200)
    assert(requestLog(0).body.contains("PATH_READ_WRITE"))
    assert(requestLog(1).body.contains("PATH_READ"))
  }

  test("default operation succeeds with PATH_READ_WRITE when write access is available") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    provider.getCredentials()

    assert(requestLog.size == 1)
    assert(requestLog.head.body.contains("PATH_READ_WRITE"))
  }

  test("read path uses PATH_READ directly when configured (no fallback)") {
    setupMockHandler(200, successResponse(accessKeyId = "READ_ONLY_KEY"))

    val readConfig = configMap + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      readConfig
    )

    val credentials = provider.getCredentials()

    assert(credentials.getAWSAccessKeyId == "READ_ONLY_KEY")
    assert(requestLog.size == 1) // Single request, no fallback
    assert(requestLog.head.body.contains("PATH_READ"))
    assert(!requestLog.head.body.contains("PATH_READ_WRITE"))
  }

  test("throws exception when both PATH_READ_WRITE and PATH_READ fail") {
    setupMockHandler(403, """{"error": "Permission denied"}""")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    val exception = intercept[RuntimeException] {
      provider.getCredentials()
    }

    assert(exception.getMessage.contains("Failed to obtain Unity Catalog credentials"))
    // Should have tried both PATH_READ_WRITE and PATH_READ
    assert(requestLog.exists(_.body.contains("PATH_READ_WRITE")))
    assert(requestLog.exists(r => r.body.contains("PATH_READ") && !r.body.contains("PATH_READ_WRITE")))
  }

  test("explicit PATH_READ throws on failure without fallback") {
    val readConfig = configMap +
      ("spark.indextables.databricks.credential.operation" -> "PATH_READ") +
      ("spark.indextables.databricks.retry.attempts"       -> "1")

    setupMockHandler(403, """{"error": "Permission denied"}""")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      readConfig
    )

    val exception = intercept[RuntimeException] {
      provider.getCredentials()
    }

    assert(exception.getMessage.contains("Failed to obtain Unity Catalog credentials"))
    // Should only have tried PATH_READ (no fallback to or from PATH_READ_WRITE)
    assert(requestLog.forall(_.body.contains("PATH_READ")))
    assert(requestLog.forall(!_.body.contains("PATH_READ_WRITE")))
  }

  // ==================== Retry Tests ====================

  test("retries on transient failures") {
    val callCount = new AtomicInteger(0)

    setupMockHandlerWithCallback { _ =>
      val num = callCount.incrementAndGet()
      if (num < 3) {
        (500, """{"error": "Internal server error"}""")
      } else {
        (200, successResponse())
      }
    }

    val testConfig = configMap + ("spark.indextables.databricks.retry.attempts" -> "3")

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      testConfig
    )

    val credentials = provider.getCredentials()

    assert(credentials != null)
    assert(credentials.getAWSAccessKeyId == "AKIAIOSFODNN7EXAMPLE")
    assert(callCount.get() == 3) // 2 failures + 1 success
  }

  // ==================== Configuration Tests ====================

  test("fails with clear error when workspace URL not configured") {
    val incompleteConfig = Map("spark.indextables.databricks.apiToken" -> "some-token")

    val exception = intercept[IllegalStateException] {
      UnityCatalogAWSCredentialProvider.fromConfig(
        new URI("s3://test-bucket/path"),
        incompleteConfig
      )
    }

    assert(exception.getMessage.contains("workspaceUrl"))
  }

  test("fails with clear error when token not configured") {
    val incompleteConfig = Map("spark.indextables.databricks.workspaceUrl" -> "https://example.com")

    val exception = intercept[IllegalStateException] {
      UnityCatalogAWSCredentialProvider.fromConfig(
        new URI("s3://test-bucket/path"),
        incompleteConfig
      )
    }

    assert(exception.getMessage.contains("databricks.apiToken"))
  }

  // ==================== Path Handling Tests ====================

  test("handles various S3 URI formats") {
    setupMockHandler(200, successResponse())

    val testUris = Seq(
      "s3://bucket/path",
      "s3://bucket/path/to/table",
      "s3a://bucket/path",
      "s3n://bucket/path"
    )

    for (uriStr <- testUris) {
      requestLog.clear()
      UnityCatalogAWSCredentialProvider.clearCache()

      val provider = UnityCatalogAWSCredentialProvider.fromConfig(
        new URI(uriStr),
        configMap
      )

      val credentials = provider.getCredentials()
      assert(credentials != null, s"Should handle URI: $uriStr")
      assert(requestLog.head.body.contains(uriStr.replace("s3a://", "s3a://").replace("s3n://", "s3n://")))
    }
  }

  test("strips trailing slash from path") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://bucket/path/"),
      configMap
    )

    provider.getCredentials()

    // Path in request should not have trailing slash
    assert(requestLog.head.body.contains("s3://bucket/path"))
    assert(!requestLog.head.body.contains("s3://bucket/path/\""))
  }

  // ==================== Refresh Tests ====================

  test("refresh() forces new API call bypassing cache") {
    setupMockHandler(200, successResponse())

    val provider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/path"),
      configMap
    )

    // First call
    provider.getCredentials()
    assert(requestLog.size == 1)

    // Second call - should use cache
    provider.getCredentials()
    assert(requestLog.size == 1)

    // Force refresh
    provider.refresh()
    assert(requestLog.size == 2, "refresh() should force a new API call")
  }

  // ==================== Table Resolution Tests ====================

  private def setupTablesHandler(responseCode: Int, responseBody: String): Unit = {
    try mockServer.removeContext("/api/2.1/unity-catalog/tables/")
    catch { case _: IllegalArgumentException => }

    val handler = new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod
        val path   = exchange.getRequestURI.getPath
        val body   = new String(exchange.getRequestBody.readAllBytes())
        val headers = scala.jdk.CollectionConverters
          .mapAsScalaMapConverter(exchange.getRequestHeaders)
          .asScala
          .map { case (k, v) => k -> v.get(0) }
          .toMap
        requestLog += MockRequest(method, path, body, headers)

        exchange.sendResponseHeaders(responseCode, responseBody.length)
        val os = exchange.getResponseBody
        os.write(responseBody.getBytes)
        os.close()
      }
    }
    // Use a root context that matches all /api/2.1/unity-catalog/tables/* paths
    mockServer.createContext("/api/2.1/unity-catalog/tables/", handler)
  }

  test("resolveTableId returns table_id from mock UC API") {
    val tableResponse =
      """{
        |  "table_id": "abc-123-uuid",
        |  "name": "my_table",
        |  "storage_location": "s3://bucket/delta_table",
        |  "table_type": "MANAGED"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    val tableId = UnityCatalogAWSCredentialProvider.resolveTableId("catalog.schema.my_table", configMap)

    tableId shouldBe "abc-123-uuid"
    requestLog.last.method shouldBe "GET"
    requestLog.last.path should include("tables")
  }

  test("resolveTableInfo returns both table_id and storage_location") {
    val tableResponse =
      """{
        |  "table_id": "def-456-uuid",
        |  "name": "events",
        |  "storage_location": "s3://my-bucket/warehouse/events",
        |  "table_type": "EXTERNAL"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    val tableInfo = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.events", configMap)

    tableInfo.tableId shouldBe "def-456-uuid"
    tableInfo.storageLocation shouldBe "s3://my-bucket/warehouse/events"
  }

  test("resolveTableInfo returns empty storageLocation when not in response") {
    val tableResponse =
      """{
        |  "table_id": "ghi-789-uuid",
        |  "name": "minimal_table"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    val tableInfo = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.minimal_table", configMap)

    tableInfo.tableId shouldBe "ghi-789-uuid"
    tableInfo.storageLocation shouldBe ""
  }

  test("resolveTableInfo caches results - second call should not make HTTP request") {
    val tableResponse =
      """{
        |  "table_id": "cache-test-uuid",
        |  "name": "cached_table",
        |  "storage_location": "s3://bucket/cached_table"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    // First call - should hit the API
    val info1 = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.cached_table", configMap)
    val requestsAfterFirst = requestLog.count(_.path.contains("tables"))

    // Second call - should use cache, not hit the API
    val info2 = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.cached_table", configMap)
    val requestsAfterSecond = requestLog.count(_.path.contains("tables"))

    info1.tableId shouldBe "cache-test-uuid"
    info2.tableId shouldBe "cache-test-uuid"
    info1.storageLocation shouldBe "s3://bucket/cached_table"
    info2.storageLocation shouldBe "s3://bucket/cached_table"
    requestsAfterFirst shouldBe 1
    requestsAfterSecond shouldBe 1 // No additional HTTP request
  }

  test("resolveTableId also benefits from table info cache") {
    val tableResponse =
      """{
        |  "table_id": "shared-cache-uuid",
        |  "name": "shared_table",
        |  "storage_location": "s3://bucket/shared_table"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    // Call resolveTableInfo first
    val info = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.shared_table", configMap)
    val requestsAfterFirst = requestLog.count(_.path.contains("tables"))

    // Call resolveTableId - should use same cache
    val tableId             = UnityCatalogAWSCredentialProvider.resolveTableId("catalog.schema.shared_table", configMap)
    val requestsAfterSecond = requestLog.count(_.path.contains("tables"))

    info.tableId shouldBe "shared-cache-uuid"
    tableId shouldBe "shared-cache-uuid"
    requestsAfterFirst shouldBe 1
    requestsAfterSecond shouldBe 1 // Shared cache, no additional request
  }

  test("table info cache is keyed by table name - different tables make separate requests") {
    val tableResponse1 =
      """{
        |  "table_id": "uuid-table-1",
        |  "name": "table1",
        |  "storage_location": "s3://bucket/table1"
        |}""".stripMargin
    val tableResponse2 =
      """{
        |  "table_id": "uuid-table-2",
        |  "name": "table2",
        |  "storage_location": "s3://bucket/table2"
        |}""".stripMargin

    // Setup handler that returns different responses based on request path
    try mockServer.removeContext("/api/2.1/unity-catalog/tables/")
    catch { case _: IllegalArgumentException => }

    val callCount = new AtomicInteger(0)
    val handler = new HttpHandler {
      override def handle(exchange: HttpExchange): Unit = {
        val method = exchange.getRequestMethod
        val path   = exchange.getRequestURI.getPath
        val body   = new String(exchange.getRequestBody.readAllBytes())
        val headers = scala.jdk.CollectionConverters
          .mapAsScalaMapConverter(exchange.getRequestHeaders)
          .asScala
          .map { case (k, v) => k -> v.get(0) }
          .toMap
        requestLog += MockRequest(method, path, body, headers)

        val responseBody = if (callCount.getAndIncrement() == 0) tableResponse1 else tableResponse2
        exchange.sendResponseHeaders(200, responseBody.length)
        val os = exchange.getResponseBody
        os.write(responseBody.getBytes)
        os.close()
      }
    }
    mockServer.createContext("/api/2.1/unity-catalog/tables/", handler)

    val info1 = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.table1", configMap)
    val info2 = UnityCatalogAWSCredentialProvider.resolveTableInfo("catalog.schema.table2", configMap)

    info1.tableId shouldBe "uuid-table-1"
    info2.tableId shouldBe "uuid-table-2"
    requestLog.count(_.path.contains("tables")) shouldBe 2 // Two different tables = two requests
  }

  // ==================== Cache Poisoning Regression Tests ====================

  test("PATH_READ cached credentials do not satisfy PATH_READ_WRITE request") {
    val callCount = new AtomicInteger(0)

    setupMockHandlerWithCallback { request =>
      val num = callCount.incrementAndGet()
      if (request.body.contains("PATH_READ_WRITE")) {
        (200, successResponse(accessKeyId = s"WRITE_KEY_$num"))
      } else {
        (200, successResponse(accessKeyId = s"READ_KEY_$num"))
      }
    }

    // First: create a PATH_READ provider and fetch credentials (populates cache)
    val readConfig = configMap + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    val readProvider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/table"),
      readConfig
    )
    val readCreds = readProvider.getCredentials()
    assert(readCreds.getAWSAccessKeyId == "READ_KEY_1")
    assert(requestLog.size == 1)
    assert(requestLog.last.body.contains("PATH_READ"))
    assert(!requestLog.last.body.contains("PATH_READ_WRITE"))

    // Second: create a PATH_READ_WRITE provider for the SAME path
    val writeConfig = configMap + ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")
    val writeProvider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/table"),
      writeConfig
    )
    val writeCreds = writeProvider.getCredentials()

    // Must get WRITE credentials, NOT the cached READ credentials
    assert(
      writeCreds.getAWSAccessKeyId == "WRITE_KEY_2",
      "PATH_READ_WRITE should get its own credentials, not reuse cached PATH_READ credentials"
    )
    assert(requestLog.size == 2, "Should have made a second API call for PATH_READ_WRITE")
    assert(requestLog.last.body.contains("PATH_READ_WRITE"))
  }

  test("PATH_READ_WRITE cached credentials do not satisfy PATH_READ request") {
    val callCount = new AtomicInteger(0)

    setupMockHandlerWithCallback { request =>
      val num = callCount.incrementAndGet()
      if (request.body.contains("PATH_READ_WRITE")) {
        (200, successResponse(accessKeyId = s"WRITE_KEY_$num"))
      } else {
        (200, successResponse(accessKeyId = s"READ_KEY_$num"))
      }
    }

    // First: create a PATH_READ_WRITE provider and fetch credentials (populates cache)
    val writeConfig = configMap + ("spark.indextables.databricks.credential.operation" -> "PATH_READ_WRITE")
    val writeProvider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/table"),
      writeConfig
    )
    val writeCreds = writeProvider.getCredentials()
    assert(writeCreds.getAWSAccessKeyId == "WRITE_KEY_1")
    assert(requestLog.size == 1)

    // Second: create a PATH_READ provider for the SAME path
    val readConfig = configMap + ("spark.indextables.databricks.credential.operation" -> "PATH_READ")
    val readProvider = UnityCatalogAWSCredentialProvider.fromConfig(
      new URI("s3://test-bucket/table"),
      readConfig
    )
    val readCreds = readProvider.getCredentials()

    // Must get READ credentials, NOT the cached WRITE credentials
    assert(
      readCreds.getAWSAccessKeyId == "READ_KEY_2",
      "PATH_READ should get its own credentials, not reuse cached PATH_READ_WRITE credentials"
    )
    assert(requestLog.size == 2, "Should have made a second API call for PATH_READ")
    assert(requestLog.last.body.contains("PATH_READ"))
    assert(!requestLog.last.body.contains("PATH_READ_WRITE"))
  }

  // ==================== Table Resolution Tests (continued) ====================

  test("resolveTableId is consistent with resolveTableInfo.tableId") {
    val tableResponse =
      """{
        |  "table_id": "same-uuid-123",
        |  "name": "table1",
        |  "storage_location": "s3://bucket/table1"
        |}""".stripMargin
    setupTablesHandler(200, tableResponse)

    val tableId   = UnityCatalogAWSCredentialProvider.resolveTableId("cat.schema.table1", configMap)
    val tableInfo = UnityCatalogAWSCredentialProvider.resolveTableInfo("cat.schema.table1", configMap)

    tableId shouldBe tableInfo.tableId
  }
}
