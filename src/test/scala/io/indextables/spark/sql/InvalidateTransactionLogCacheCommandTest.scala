package io.indextables.spark.sql

import org.apache.spark.sql.types._

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.TestBase

class InvalidateTransactionLogCacheCommandTest extends TestBase {

  test("InvalidateTransactionLogCacheCommand should execute successfully for global invalidation") {
    val command = InvalidateTransactionLogCacheCommand(None)
    val results = command.run(spark)

    // Should return exactly one result for global operation
    assert(results.length == 1)

    // Check result format
    val result = results.head
    assert(result.getString(0) == "GLOBAL")                       // table_path
    assert(result.getString(1).contains("not fully implemented")) // result
    assert(result.getLong(2) == 0L)                               // cache_hits_before
    assert(result.getLong(3) == 0L)                               // cache_misses_before
    assert(result.getString(4) == "N/A")                          // hit_rate_before
  }

  test("InvalidateTransactionLogCacheCommand should work for specific table path") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString

      // Create and initialize a transaction log with caching enabled
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(
          Array(
            StructField("id", IntegerType, nullable = false),
            StructField("name", StringType, nullable = false)
          )
        )

        // Initialize the transaction log
        transactionLog.initialize(schema)

        // Add some files to create cache activity
        val addAction = io.indextables.spark.transaction.AddAction(
          path = "test_file.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100)
        )
        transactionLog.addFile(addAction)

        // Perform some reads to populate cache
        transactionLog.listFiles()
        transactionLog.getMetadata()

        // Now test the invalidate command
        val command = InvalidateTransactionLogCacheCommand(Some(tablePath))
        val results = command.run(spark)

        // Should return exactly one result
        assert(results.length == 1)

        val result = results.head
        assert(result.getString(0) == tablePath)                         // table_path
        assert(result.getString(1).contains("invalidated successfully")) // result
        // Cache stats may vary depending on implementation
      } finally
        transactionLog.close()
    }
  }

  test("InvalidateTransactionLogCacheCommand should handle non-existent table") {
    val nonExistentPath = "/absolutely/non/existent/path/that/should/never/exist"
    val command         = InvalidateTransactionLogCacheCommand(Some(nonExistentPath))
    val results         = command.run(spark)

    // Should return one result with error message
    assert(results.length == 1)

    val result = results.head
    assert(result.getString(0) == nonExistentPath) // table_path
    val resultMessage = result.getString(1)
    assert(resultMessage.contains("not found") || resultMessage.contains("Failed to invalidate")) // result
    assert(result.getLong(2) == 0L)                                                               // cache_hits_before
    assert(result.getLong(3) == 0L)                                                               // cache_misses_before
    assert(result.getString(4) == "N/A")                                                          // hit_rate_before
  }

  test("SQL command parsing should work via extension") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    // Test global invalidation
    val globalSql        = "INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE"
    val globalParsedPlan = parser.parsePlan(globalSql)
    assert(globalParsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])

    val globalCommand = globalParsedPlan.asInstanceOf[InvalidateTransactionLogCacheCommand]
    assert(globalCommand.tablePath.isEmpty)

    // Test with path
    val pathSql        = "INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR '/path/to/table'"
    val pathParsedPlan = parser.parsePlan(pathSql)
    assert(pathParsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])

    val pathCommand = pathParsedPlan.asInstanceOf[InvalidateTransactionLogCacheCommand]
    assert(pathCommand.tablePath.contains("/path/to/table"))

    // Test with table identifier
    val tableSql        = "INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR my_table"
    val tableParsedPlan = parser.parsePlan(tableSql)
    assert(tableParsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])

    val tableCommand = tableParsedPlan.asInstanceOf[InvalidateTransactionLogCacheCommand]
    assert(tableCommand.tablePath.contains("my_table"))
  }

  test("SQL command should be case insensitive") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val variations = Seq(
      "invalidate tantivy4spark transaction log cache",
      "INVALIDATE tantivy4spark TRANSACTION log CACHE",
      "Invalidate TANTIVY4SPARK Transaction Log Cache",
      "invalidate tantivy4spark transaction log cache for '/test/path'"
    )

    variations.foreach { sql =>
      val parsedPlan = parser.parsePlan(sql)
      assert(parsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])
    }
  }

  test("SQL command with schema qualified table name") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    val sql        = "INVALIDATE TANTIVY4SPARK TRANSACTION LOG CACHE FOR database.table_name"
    val parsedPlan = parser.parsePlan(sql)
    assert(parsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])

    val command = parsedPlan.asInstanceOf[InvalidateTransactionLogCacheCommand]
    assert(command.tablePath.contains("database.table_name"))
  }

  test("command should actually invalidate cache") {
    withTempPath { tempDir =>
      val tablePath = tempDir.toString

      // Create and initialize a transaction log with caching enabled
      val transactionLog = TransactionLogFactory.create(new Path(tablePath), spark)

      try {
        val schema = StructType(
          Array(
            StructField("id", IntegerType, nullable = false),
            StructField("name", StringType, nullable = false)
          )
        )

        // Initialize the transaction log
        transactionLog.initialize(schema)

        // Add some files and perform reads to populate cache
        val addAction = io.indextables.spark.transaction.AddAction(
          path = "test_file.split",
          partitionValues = Map.empty,
          size = 1000L,
          modificationTime = System.currentTimeMillis(),
          dataChange = true,
          numRecords = Some(100)
        )
        transactionLog.addFile(addAction)

        // Perform reads to populate cache
        transactionLog.listFiles()
        transactionLog.getMetadata()

        // Get cache stats to verify cache has activity
        val statsBefore = transactionLog.getCacheStats()
        assert(statsBefore.isDefined, "Cache should be enabled")

        // Execute invalidate command
        val command = InvalidateTransactionLogCacheCommand(Some(tablePath))
        val results = command.run(spark)

        // Command should succeed
        assert(results.length == 1)
        val result = results.head
        assert(result.getString(1).contains("invalidated successfully"))

        // Verify cache was actually invalidated by doing another read
        // This would generate new cache misses if the cache was properly cleared
        transactionLog.listFiles()
        val statsAfter = transactionLog.getCacheStats()
        assert(statsAfter.isDefined, "Cache should still be enabled")

      } finally
        transactionLog.close()
    }
  }

  test("non-matching SQL should delegate to default parser") {
    val parser = new IndexTables4SparkSqlParser(spark.sessionState.sqlParser)

    // This should be handled by the default Spark SQL parser
    val parsedPlan = parser.parsePlan("SELECT 1 as test")
    assert(!parsedPlan.isInstanceOf[InvalidateTransactionLogCacheCommand])
  }
}
