import org.apache.spark.sql.SparkSession

object TestGroupByValidation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("TestGroupByValidation")
      .master("local[*]")
      .getOrCreate()

    try {
      import spark.implicits._

      // Test data
      val testData = Seq(
        ("doc1", "category_a", 1),
        ("doc2", "category_a", 2),
        ("doc3", "category_b", 3)
      ).toDF("id", "category", "value")

      val tempDir   = java.nio.file.Files.createTempDirectory("groupby-validation-test").toFile
      val tablePath = tempDir.getAbsolutePath

      println("🧪 Test 1: GROUP BY field without fast field configuration (should be rejected)")

      // Write data WITHOUT configuring 'category' as fast field
      testData.write
        .format("tantivy4spark")
        .option("spark.indextables.indexing.typemap.category", "string")
        // Note: NOT adding category to fastfields - this should cause rejection
        .mode("overwrite")
        .save(tablePath)

      val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(tablePath)

      // This should be rejected by our validation
      val query = df.groupBy("category").count()

      // Check the physical plan
      val physicalPlan = query.queryExecution.executedPlan.toString
      println("🔍 Physical plan:")
      println(physicalPlan)

      if (physicalPlan.contains("HashAggregate")) {
        println("✅ EXPECTED: GROUP BY pushdown was rejected, falling back to HashAggregate")
      } else if (physicalPlan.contains("IndexTables4SparkGroupByAggregateScan")) {
        println("❌ UNEXPECTED: GROUP BY pushdown was accepted despite missing fast field")
      } else {
        println("? UNKNOWN: Physical plan doesn't contain expected patterns")
      }

      // Clean up
      def deleteRecursively(file: java.io.File): Unit = {
        if (file.isDirectory) {
          file.listFiles().foreach(deleteRecursively)
        }
        file.delete()
      }
      deleteRecursively(tempDir)

    } finally
      spark.stop()
  }
}
