package io.indextables.spark.transaction.avro

import java.nio.file.Files

import io.indextables.spark.TestBase

class PurgeBugReproTest extends TestBase {

  test("PURGE should not identify newly written files as orphaned") {
    val testDir   = Files.createTempDirectory("purge-bug-test").toString
    val tablePath = s"$testDir/test_table"

    val spark = this.spark
    import spark.implicits._

    // Create a simple table with some data
    val df = Seq(
      (1, "Alice", 100),
      (2, "Bob", 200),
      (3, "Carol", 300)
    ).toDF("id", "name", "value")

    // Write the data
    df.write
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .mode("overwrite")
      .save(tablePath)

    println(s"=== Table written to $tablePath ===")

    // List the split files
    val splitFiles = new java.io.File(tablePath)
      .listFiles()
      .filter(_.getName.endsWith(".split"))
    println(s"Split files written: ${splitFiles.length}")
    splitFiles.foreach(f => println(s"  - ${f.getName}"))

    // Read back to verify
    val readDf = spark.read
      .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(tablePath)
    val count = readDf.count()
    println(s"Row count: $count")
    assert(count == 3, s"Expected 3 rows but got $count")

    // Now run PURGE with DRY RUN
    println("\n=== Running PURGE DRY RUN ===")
    val purgeResult = spark.sql(s"PURGE INDEXTABLE '$tablePath' DRY RUN")
    purgeResult.show(false)

    val purgeRows     = purgeResult.collect()
    val metrics       = purgeRows.head.getAs[org.apache.spark.sql.Row]("metrics")
    val orphanedFiles = metrics.getAs[Long]("orphaned_files_found")

    println(s"\nOrphaned files identified: $orphanedFiles")

    // This should be 0 - no files should be orphaned in a brand new table
    assert(orphanedFiles == 0, s"Expected 0 orphaned files but found $orphanedFiles - THIS IS THE BUG!")

    println("✅ Test passed - no files incorrectly identified as orphaned")
  }
}
