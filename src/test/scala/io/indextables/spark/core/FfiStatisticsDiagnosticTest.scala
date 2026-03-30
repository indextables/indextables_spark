package io.indextables.spark.core

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.TestBase

/** Diagnostic test to verify FFI statistics are populated correctly. */
class FfiStatisticsDiagnosticTest extends TestBase {

  private val format = INDEXTABLES_FORMAT

  test("Arrow FFI statistics are populated correctly") {
    withTempPath { ffiPath =>
      val sparkImplicits = spark.implicits
      import sparkImplicits._

      val data = Seq(
        ("doc1", "report.json"),
        ("doc2", "data.csv"),
        ("doc3", "config.json"),
        ("doc4", "readme.txt"),
        ("doc5", "schema.json")
      ).toDF("id", "filename")

      // Write via Arrow FFI (the only write path)
      data.write
        .format(format)
        .option("spark.indextables.indexing.typemap.filename", "string")
        .mode("overwrite")
        .save(ffiPath.toString)

      val ffiActions = TransactionLogFactory
        .create(new Path(ffiPath.toString), spark, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
        .listFiles()

      println("=" * 60)
      println("FFI STATISTICS DIAGNOSTIC")
      println("=" * 60)

      ffiActions.foreach { a =>
        println(s"FFI:  path=${a.path}")
        println(s"  numRecords=${a.numRecords}")
        println(s"  minValues=${a.minValues}")
        println(s"  maxValues=${a.maxValues}")
        a.minValues.foreach { mv =>
          mv.foreach {
            case (k, v) =>
              println(s"  min[$k] = '$v' (length=${v.length}, empty=${v.isEmpty})")
          }
        }
        a.maxValues.foreach { mv =>
          mv.foreach {
            case (k, v) =>
              println(s"  max[$k] = '$v' (length=${v.length}, empty=${v.isEmpty})")
          }
        }
      }

      // Verify FFI has statistics
      ffiActions.foreach { a =>
        a.numRecords shouldBe defined
        a.minValues shouldBe defined
        a.maxValues shouldBe defined
      }

      println("=" * 60)
    }
  }
}
