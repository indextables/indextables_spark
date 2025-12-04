package io.indextables.spark.debug

import io.indextables.spark.TestBase
import io.indextables.spark.transaction.TransactionLogFactory
import org.apache.hadoop.fs.Path

class FastFieldDebugTest extends TestBase {

  test("check docMappingJson fast fields") {
    withTempPath { testPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      // Create test data matching the failing test
      val df = spark.range(0, 100).toDF("id")
        .selectExpr("id", "id % 100 as group_id", "id * 2 as value", "concat('item_', id) as name")

      println(s"Schema: ${df.schema.fields.map(f => s"${f.name}:${f.dataType}").mkString(", ")}")

      df.write
        .format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .mode("overwrite")
        .save(testPath)

      println("\n=== CHECKING docMappingJson ===")

      // Read transaction log using TransactionLog API
      val txLog = TransactionLogFactory.create(new Path(testPath), spark)
      val files = txLog.listFiles()

      println(s"Found ${files.length} files in transaction log")

      files.take(1).foreach { addAction =>
        println(s"\nSplit: ${addAction.path}")
        addAction.docMappingJson match {
          case Some(docMapping) =>
            println(s"docMappingJson: $docMapping")

            // Parse to show fast fields nicely
            import com.fasterxml.jackson.databind.ObjectMapper
            val mapper = new ObjectMapper()
            val node = mapper.readTree(docMapping)
            if (node.isArray) {
              import scala.jdk.CollectionConverters._
              println("\n=== FAST FIELD STATUS ===")
              node.elements().asScala.foreach { field =>
                val name = Option(field.get("name")).map(_.asText()).getOrElse("?")
                val fast = Option(field.get("fast")).map(_.asBoolean()).getOrElse(false)
                val fieldType = Option(field.get("type")).map(_.asText()).getOrElse("?")
                println(s"  $name: fast=$fast, type=$fieldType")
              }
            }
          case None =>
            println("NO docMappingJson found!")
        }
      }
    }
  }
}
