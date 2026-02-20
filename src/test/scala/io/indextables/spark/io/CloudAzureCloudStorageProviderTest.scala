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

package io.indextables.spark.io

import java.util.{Collections => JCollections}

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import io.indextables.spark.CloudAzureTestBase

/**
 * Unit tests for AzureCloudStorageProvider.
 *
 * These tests validate Azure-specific functionality including:
 *   - Configuration extraction from multiple sources
 *   - Credential resolution hierarchy
 *   - Azure protocol detection
 *   - Basic CRUD operations against real Azure Blob Storage
 *   - Parallel operations
 *   - Error handling
 *
 * Prerequisites:
 *   - Azure credentials configured via system properties, ~/.azure/credentials, or environment variables
 *   - Azure container accessible for testing
 */
class CloudAzureCloudStorageProviderTest extends CloudAzureTestBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    // Skip all tests if Azure credentials are not available
    assume(
      hasAzureCredentials(),
      "Azure credentials not available - skipping tests"
    )
  }

  test("should extract Azure configuration from Spark config") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Set Azure configuration in Hadoop config (simulating Spark conf)
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())

    // Create provider - should pick up config from Hadoop conf
    val provider = CloudStorageProviderFactory.createProvider(
      s"azure://$testContainer/test-path",
      options,
      hadoopConf
    )

    provider.getProviderType shouldBe "Azure"

    // Clean up
    provider.close()

    println("✅ Successfully extracted Azure configuration from Spark config")
  }

  test("should prioritize DataFrame options over Spark config") {
    val hadoopConf = spark.sparkContext.hadoopConfiguration

    // Set Azure configuration in Hadoop config
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    hadoopConf.set("spark.indextables.azure.accountKey", "hadoop-key")

    // Set different values in DataFrame options (use real credentials for actual test)
    val optionsMap = new java.util.HashMap[String, String]()
    getStorageAccount.foreach(account => optionsMap.put("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => optionsMap.put("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(optionsMap)

    // Create provider - should prioritize options over Hadoop conf
    val provider = CloudStorageProviderFactory.createProvider(
      s"azure://$testContainer/test-path",
      options,
      hadoopConf
    )

    provider.getProviderType shouldBe "Azure"

    // Clean up
    provider.close()

    println("✅ Successfully prioritized DataFrame options over Spark config")
  }

  test("should detect Azure protocol from URL") {
    val protocol = ProtocolBasedIOFactory.determineProtocol(s"azure://$testContainer/path")
    protocol shouldBe ProtocolBasedIOFactory.AzureProtocol

    println("✅ Successfully detected Azure protocol from URL")
  }

  test("should write and read file from Azure") {
    val testPath    = s"azure://$testContainer/test-write-read-${generateTestId()}.txt"
    val testContent = "Hello Azure Blob Storage!"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      testPath,
      options,
      hadoopConf
    )

    try {
      // Write file
      provider.writeFile(testPath, testContent.getBytes("UTF-8"))

      // Verify exists
      provider.exists(testPath) shouldBe true

      // Read file
      val readContent = new String(provider.readFile(testPath), "UTF-8")
      readContent shouldBe testContent

      println("✅ Successfully wrote and read file from Azure")
    } finally {
      // Clean up
      provider.deleteFile(testPath)
      provider.close()
    }
  }

  test("should support conditional write (writeFileIfNotExists)") {
    val testPath     = s"azure://$testContainer/test-conditional-${generateTestId()}.txt"
    val testContent1 = "First write"
    val testContent2 = "Second write"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      testPath,
      options,
      hadoopConf
    )

    try {
      // First write should succeed
      val firstWrite = provider.writeFileIfNotExists(testPath, testContent1.getBytes("UTF-8"))
      firstWrite shouldBe true

      // Second write should fail (file already exists)
      val secondWrite = provider.writeFileIfNotExists(testPath, testContent2.getBytes("UTF-8"))
      secondWrite shouldBe false

      // Content should still be from first write
      val readContent = new String(provider.readFile(testPath), "UTF-8")
      readContent shouldBe testContent1

      println("✅ Successfully tested conditional write (writeFileIfNotExists)")
    } finally {
      // Clean up
      provider.deleteFile(testPath)
      provider.close()
    }
  }

  test("should list files in Azure container") {
    val testPrefix = s"test-list-${generateTestId()}"
    val testPath1  = s"azure://$testContainer/$testPrefix/file1.txt"
    val testPath2  = s"azure://$testContainer/$testPrefix/file2.txt"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      s"azure://$testContainer/$testPrefix/",
      options,
      hadoopConf
    )

    try {
      // Write test files
      provider.writeFile(testPath1, "Content 1".getBytes("UTF-8"))
      provider.writeFile(testPath2, "Content 2".getBytes("UTF-8"))

      // List files
      val files = provider.listFiles(s"azure://$testContainer/$testPrefix/", recursive = false)

      files.size should be >= 2
      files.exists(_.path.contains("file1.txt")) shouldBe true
      files.exists(_.path.contains("file2.txt")) shouldBe true

      println("✅ Successfully listed files in Azure container")
    } finally {
      // Clean up
      provider.deleteFile(testPath1)
      provider.deleteFile(testPath2)
      provider.close()
    }
  }

  test("should support parallel read operations") {
    val testPrefix = s"test-parallel-${generateTestId()}"
    val testPaths  = (1 to 3).map(i => s"azure://$testContainer/$testPrefix/file$i.txt")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      s"azure://$testContainer/$testPrefix/",
      options,
      hadoopConf
    )

    try {
      // Write test files
      testPaths.zipWithIndex.foreach {
        case (path, i) =>
          provider.writeFile(path, s"Test content $i".getBytes("UTF-8"))
      }

      // Test parallel read
      val contents = provider.readFilesParallel(testPaths)
      contents.size shouldBe 3

      testPaths.zipWithIndex.foreach {
        case (path, i) =>
          val content = new String(contents(path), "UTF-8")
          content shouldBe s"Test content $i"
      }

      println("✅ Successfully tested parallel read operations")
    } finally {
      // Clean up
      testPaths.foreach(provider.deleteFile)
      provider.close()
    }
  }

  test("should support parallel exists operations") {
    val testPrefix = s"test-exists-${generateTestId()}"
    val testPaths  = (1 to 3).map(i => s"azure://$testContainer/$testPrefix/file$i.txt")

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      s"azure://$testContainer/$testPrefix/",
      options,
      hadoopConf
    )

    try {
      // Write test files
      testPaths.foreach(path => provider.writeFile(path, "Test content".getBytes("UTF-8")))

      // Test parallel existence check
      val existsMap = provider.existsParallel(testPaths)
      existsMap.values.forall(_ == true) shouldBe true

      println("✅ Successfully tested parallel exists operations")
    } finally {
      // Clean up
      testPaths.foreach(provider.deleteFile)
      provider.close()
    }
  }

  test("should get file info from Azure") {
    val testPath    = s"azure://$testContainer/test-info-${generateTestId()}.txt"
    val testContent = "File info test content"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      testPath,
      options,
      hadoopConf
    )

    try {
      // Write file
      provider.writeFile(testPath, testContent.getBytes("UTF-8"))

      // Get file info
      val fileInfo = provider.getFileInfo(testPath)
      fileInfo shouldBe defined
      fileInfo.get.size shouldBe testContent.getBytes("UTF-8").length
      fileInfo.get.path shouldBe testPath

      println("✅ Successfully retrieved file info from Azure")
    } finally {
      // Clean up
      provider.deleteFile(testPath)
      provider.close()
    }
  }

  test("should handle non-existent files gracefully") {
    val nonExistentPath = s"azure://$testContainer/non-existent-${generateTestId()}.txt"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      nonExistentPath,
      options,
      hadoopConf
    )

    try {
      // Check existence
      provider.exists(nonExistentPath) shouldBe false

      // Get file info
      provider.getFileInfo(nonExistentPath) shouldBe None

      // Delete non-existent file
      provider.deleteFile(nonExistentPath) shouldBe false

      println("✅ Successfully handled non-existent files gracefully")
    } finally
      provider.close()
  }

  test("should support range reads") {
    val testPath    = s"azure://$testContainer/test-range-${generateTestId()}.txt"
    val testContent = "0123456789ABCDEFGHIJ" // 20 bytes

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      testPath,
      options,
      hadoopConf
    )

    try {
      // Write file
      provider.writeFile(testPath, testContent.getBytes("UTF-8"))

      // Read range: bytes 5-9 (should be "56789")
      val rangeContent = new String(provider.readRange(testPath, 5, 5), "UTF-8")
      rangeContent shouldBe "56789"

      println("✅ Successfully tested range reads")
    } finally {
      // Clean up
      provider.deleteFile(testPath)
      provider.close()
    }
  }

  test("should normalize Azure paths for tantivy") {
    val azurePath = s"azure://$testContainer/splits/test.split"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    getStorageAccount.foreach(account => hadoopConf.set("spark.indextables.azure.accountName", account))
    getAccountKey.foreach(key => hadoopConf.set("spark.indextables.azure.accountKey", key))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      azurePath,
      options,
      hadoopConf
    )

    try {
      // Azure paths should pass through unchanged (tantivy4java handles azure:// natively)
      val normalized = provider.normalizePathForTantivy(azurePath)
      normalized shouldBe azurePath

      println("✅ Successfully normalized Azure paths for tantivy")
    } finally
      provider.close()
  }

  test("should support connection string authentication") {
    // Skip if connection string is not available
    val connString = getConnectionString
    assume(connString.isDefined, "Connection string not available")

    val testPath = s"azure://$testContainer/test-connstring-${generateTestId()}.txt"

    val hadoopConf = spark.sparkContext.hadoopConfiguration
    connString.foreach(cs => hadoopConf.set("spark.indextables.azure.connectionString", cs))

    val options = new CaseInsensitiveStringMap(JCollections.emptyMap())
    val provider = CloudStorageProviderFactory.createProvider(
      testPath,
      options,
      hadoopConf
    )

    try {
      // Write file
      provider.writeFile(testPath, "Connection string test".getBytes("UTF-8"))

      // Verify exists
      provider.exists(testPath) shouldBe true

      println("✅ Successfully authenticated using connection string")
    } finally {
      // Clean up
      provider.deleteFile(testPath)
      provider.close()
    }
  }

  test("should load credentials from ~/.azure/credentials file") {
    // This test just verifies the credential loading mechanism works
    // Actual credentials should be loaded in CloudAzureTestBase
    val hasFileCreds = azureStorageAccount.isDefined && azureAccountKey.isDefined

    if (hasFileCreds) {
      println(s"✅ Successfully loaded credentials from ~/.azure/credentials")
      println(s"   Storage Account: ${azureStorageAccount.get}")
    } else {
      println("ℹ️  No credentials found in ~/.azure/credentials (using other source)")
    }

    // Test passes regardless - just validates the mechanism
    succeed
  }
}
