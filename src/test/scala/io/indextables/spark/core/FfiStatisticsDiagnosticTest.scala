package io.indextables.spark.core

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.util.CaseInsensitiveStringMap

import org.apache.hadoop.fs.Path

import io.indextables.spark.transaction.TransactionLogFactory
import io.indextables.spark.TestBase

/** Diagnostic test to confirm FFI statistics are empty (tantivy4java bug). */
class FfiStatisticsDiagnosticTest extends TestBase {

  private val format = INDEXTABLES_FORMAT

  test("FFI statistics are empty — tantivy4java bug in Arrow FFI ingestion path") {
    withTempPath { tantPath =>
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

        // Write via TANT batch
        data.write
          .format(format)
          .option("spark.indextables.indexing.typemap.filename", "string")
          .option("spark.indextables.write.arrowFfi.enabled", "false")
          .mode("overwrite")
          .save(tantPath.toString)

        // Write via Arrow FFI
        data.write
          .format(format)
          .option("spark.indextables.indexing.typemap.filename", "string")
          .option("spark.indextables.write.arrowFfi.enabled", "true")
          .mode("overwrite")
          .save(ffiPath.toString)

        val tantActions = TransactionLogFactory
          .create(new Path(tantPath.toString), spark, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
          .listFiles()
        val ffiActions = TransactionLogFactory
          .create(new Path(ffiPath.toString), spark, new CaseInsensitiveStringMap(java.util.Collections.emptyMap()))
          .listFiles()

        println("=" * 60)
        println("FFI STATISTICS DIAGNOSTIC")
        println("=" * 60)

        tantActions.foreach { a =>
          println(s"TANT: path=${a.path}")
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

        // Verify TANT has correct statistics
        tantActions.foreach { a =>
          a.minValues.foreach { mv =>
            mv.foreach {
              case (k, v) =>
                assert(v.nonEmpty, s"TANT min[$k] should not be empty")
            }
          }
        }

        // Verify FFI has empty statistics (documenting the bug)
        val hasEmptyStats = ffiActions.exists(a => a.minValues.exists(_.values.exists(_.isEmpty)))
        println(s"\nFFI has empty statistics: $hasEmptyStats")
        println("=" * 60)

        if (hasEmptyStats) {
          println("""
*** tantivy4java BUG: Arrow FFI path returns empty statistics ***
The finishAllSplitsRaw result contains field names in minValues/maxValues
maps but with empty string values. This breaks data skipping:
  - StringEndsWith: "" == "" → true, !"".endsWith(x) → true → SKIP (wrong!)
  - StringContains: "" == "" → true, !"".contains(x) → true → SKIP (wrong!)
This needs to be fixed in the Rust Arrow FFI ingestion path.
""")
        }
      }
    }
  }
}
