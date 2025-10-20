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

package io.indextables.spark

import java.io.File
import java.nio.file.{Files, Paths}

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.SparkSession

import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

/**
 * Base class for real Azure Blob Storage integration tests.
 *
 * This base class provides Spark session setup without any Azurite mock configuration, allowing tests to connect to
 * real Azure Blob Storage using standard Azure credentials.
 *
 * Credentials are loaded from multiple sources with the following priority:
 * 1. System properties: test.azure.storageAccount, test.azure.accountKey, test.azure.container
 * 2. ~/.azure/credentials file (matches tantivy4java pattern)
 * 3. Environment variables: AZURE_STORAGE_ACCOUNT, AZURE_STORAGE_KEY
 *
 * Unlike TestBase, this class does NOT set any Azurite mock endpoints or configurations that would interfere with real
 * Azure Blob Storage connections.
 */
abstract class RealAzureTestBase extends AnyFunSuite with Matchers with BeforeAndAfterAll with BeforeAndAfterEach {

  protected var spark: SparkSession = _

  // Test configuration from system properties
  protected val testContainer: String =
    Option(System.getProperty("test.azure.container")).getOrElse("tantivy4spark-test")

  protected val storageAccount: Option[String] = Option(System.getProperty("test.azure.storageAccount"))
  protected val accountKey: Option[String]     = Option(System.getProperty("test.azure.accountKey"))

  // Azure credentials loaded from ~/.azure/credentials
  protected var azureStorageAccount: Option[String] = None
  protected var azureAccountKey: Option[String]     = None
  protected var azureTenantId: Option[String]       = None
  protected var azureClientId: Option[String]       = None
  protected var azureClientSecret: Option[String]   = None

  /**
   * Load Azure credentials from ~/.azure/credentials file (matches tantivy4java pattern)
   *
   * File format:
   * {{{
   * [default]
   * storage_account = yourstorageaccount
   * account_key = your-account-key-here
   * # OAuth Service Principal (optional)
   * tenant_id = your-tenant-id
   * client_id = your-client-id
   * client_secret = your-client-secret
   * }}}
   */
  private def loadAzureCredentialsFromFile(): Unit =
    try {
      val credentialsPath = Paths.get(System.getProperty("user.home"), ".azure", "credentials")
      if (!Files.exists(credentialsPath)) {
        println(s"Azure credentials file not found at: $credentialsPath")
        return
      }

      val lines           = Files.readAllLines(credentialsPath).asScala
      var inDefaultSection = false

      for (line <- lines) {
        val trimmed = line.trim
        if (trimmed == "[default]") {
          inDefaultSection = true
        } else if (trimmed.startsWith("[") && trimmed.endsWith("]")) {
          inDefaultSection = false
        } else if (inDefaultSection && trimmed.contains("=")) {
          val parts = trimmed.split("=", 2)
          if (parts.length == 2) {
            val key   = parts(0).trim
            val value = parts(1).trim

            key match {
              case "storage_account" => azureStorageAccount = Some(value)
              case "account_key"     => azureAccountKey = Some(value)
              case "tenant_id"       => azureTenantId = Some(value)
              case "client_id"       => azureClientId = Some(value)
              case "client_secret"   => azureClientSecret = Some(value)
              case _                 => // Ignore unknown keys
            }
          }
        }
      }

      val hasAccountKey = azureStorageAccount.isDefined && azureAccountKey.isDefined
      val hasOAuth = azureStorageAccount.isDefined && azureTenantId.isDefined &&
                     azureClientId.isDefined && azureClientSecret.isDefined

      if (hasAccountKey) {
        println("✅ Loaded Azure account key credentials from ~/.azure/credentials")
      }
      if (hasOAuth) {
        println("✅ Loaded Azure OAuth Service Principal credentials from ~/.azure/credentials")
      }
      if (!hasAccountKey && !hasOAuth && azureStorageAccount.isDefined) {
        println("⚠️  Azure credentials incomplete in ~/.azure/credentials file")
      }
    } catch {
      case ex: Exception =>
        println(s"Failed to read Azure credentials from file: ${ex.getMessage}")
    }

  /**
   * Get effective storage account from multiple sources: 1. System property 2. ~/.azure/credentials file 3.
   * Environment variable
   */
  protected def getStorageAccount: Option[String] =
    storageAccount
      .orElse(azureStorageAccount)
      .orElse(Option(System.getenv("AZURE_STORAGE_ACCOUNT")))

  /**
   * Get effective account key from multiple sources: 1. System property 2. ~/.azure/credentials file 3. Environment
   * variable
   */
  protected def getAccountKey: Option[String] =
    accountKey
      .orElse(azureAccountKey)
      .orElse(Option(System.getenv("AZURE_STORAGE_KEY")))

  /**
   * Get effective connection string from environment variable (if set)
   */
  protected def getConnectionString: Option[String] =
    Option(System.getenv("AZURE_STORAGE_CONNECTION_STRING"))

  /**
   * Get effective tenant ID from multiple sources: 1. System property 2. ~/.azure/credentials file 3. Environment
   * variable
   */
  protected def getTenantId: Option[String] =
    Option(System.getProperty("test.azure.tenantId"))
      .orElse(azureTenantId)
      .orElse(Option(System.getenv("AZURE_TENANT_ID")))

  /**
   * Get effective client ID from multiple sources: 1. System property 2. ~/.azure/credentials file 3. Environment
   * variable
   */
  protected def getClientId: Option[String] =
    Option(System.getProperty("test.azure.clientId"))
      .orElse(azureClientId)
      .orElse(Option(System.getenv("AZURE_CLIENT_ID")))

  /**
   * Get effective client secret from multiple sources: 1. System property 2. ~/.azure/credentials file 3. Environment
   * variable
   */
  protected def getClientSecret: Option[String] =
    Option(System.getProperty("test.azure.clientSecret"))
      .orElse(azureClientSecret)
      .orElse(Option(System.getenv("AZURE_CLIENT_SECRET")))

  /**
   * Check if OAuth Service Principal credentials are available
   */
  protected def hasOAuthCredentials(): Boolean =
    getTenantId.isDefined && getClientId.isDefined && getClientSecret.isDefined

  /**
   * Check if real Azure credentials are available from any source. This is a utility method for tests to check
   * credential availability before running.
   */
  protected def hasAzureCredentials(): Boolean = {
    loadAzureCredentialsFromFile()

    val hasExplicitCreds = storageAccount.isDefined && accountKey.isDefined
    val hasFileCreds     = azureStorageAccount.isDefined && azureAccountKey.isDefined
    val hasEnvAccount    = Option(System.getenv("AZURE_STORAGE_ACCOUNT")).isDefined
    val hasConnString    = Option(System.getenv("AZURE_STORAGE_CONNECTION_STRING")).isDefined

    hasExplicitCreds || hasFileCreds || hasEnvAccount || hasConnString
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Clear global split cache before each test to avoid schema pollution
    try {
      import _root_.io.indextables.spark.storage.{GlobalSplitCacheManager, SplitLocationRegistry}
      GlobalSplitCacheManager.flushAllCaches()
      SplitLocationRegistry.clearAllLocations()
    } catch {
      case _: Exception => // Ignore if cache clearing fails
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Load credentials from file
    loadAzureCredentialsFromFile()

    // Verify credentials are available
    if (!hasAzureCredentials()) {
      println("⚠️  No Azure credentials found. Tests may be skipped.")
      println("   Configure one of:")
      println("   - System properties: -Dtest.azure.storageAccount=... -Dtest.azure.accountKey=...")
      println("   - ~/.azure/credentials file with [default] section")
      println("   - Environment: AZURE_STORAGE_ACCOUNT and AZURE_STORAGE_KEY")
      println("   - Environment: AZURE_STORAGE_CONNECTION_STRING")
    } else {
      val source = if (storageAccount.isDefined && accountKey.isDefined) {
        "system properties"
      } else if (azureStorageAccount.isDefined && azureAccountKey.isDefined) {
        "~/.azure/credentials"
      } else if (getConnectionString.isDefined) {
        "AZURE_STORAGE_CONNECTION_STRING"
      } else {
        "environment variables"
      }
      println(s"✅ Using Azure credentials from $source")
    }

    // Create Spark session without any Azurite mock configuration
    val builder = SparkSession
      .builder()
      .appName("RealAzureIntegrationTest")
      .master("local[2]")
      .config("spark.sql.warehouse.dir", Files.createTempDirectory("spark-warehouse").toString)
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.adaptive.enabled", "false")
      .config("spark.sql.adaptive.coalescePartitions.enabled", "false")
      // Enable IndexTables4Spark extensions for IndexQuery support
      .config("spark.sql.extensions", "io.indextables.spark.extensions.IndexTables4SparkExtensions")

    // Configure Azure credentials if available
    getStorageAccount.foreach { account =>
      builder.config("spark.indextables.azure.accountName", account)
    }

    getAccountKey.foreach { key =>
      builder.config("spark.indextables.azure.accountKey", key)
    }

    getConnectionString.foreach { connStr =>
      builder.config("spark.indextables.azure.connectionString", connStr)
    }

    spark = builder.getOrCreate()

    // Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")

    println(s"🚀 RealAzureTestBase: Spark session created for real Azure Blob Storage testing")
  }

  override def afterAll(): Unit = {
    if (spark != null) {
      spark.stop()
      println(s"🛑 RealAzureTestBase: Spark session stopped")
    }
    super.afterAll()
  }

  /** Create a temporary directory for test data. */
  protected def withTempPath(f: File => Unit): Unit = {
    val tempDir = Files.createTempDirectory("tantivy4spark-test")
    try
      f(tempDir.toFile)
    finally {
      // Clean up temp directory
      def deleteRecursively(file: File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir.toFile)
    }
  }

  /** Generate a unique test ID for avoiding conflicts in Azure. */
  protected def generateTestId(): String =
    java.util.UUID.randomUUID().toString.substring(0, 8)

  /**
   * Get the effective Azure endpoint (if configured). By default, uses standard Azure endpoint unless overridden for
   * Azurite or custom endpoints.
   */
  protected def getAzureEndpoint: Option[String] =
    Option(System.getProperty("test.azure.endpoint"))
      .orElse(Option(System.getenv("AZURE_STORAGE_ENDPOINT")))
}
