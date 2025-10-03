package io.indextables.spark.transaction

import io.indextables.spark.TestBase
import org.scalatest.matchers.should.Matchers._

/**
 * Test to reproduce and validate transaction log file naming bug.
 * This test verifies that file names follow the Delta Lake protocol format.
 */
class TransactionLogFileNamingTest extends TestBase {

  test("version formatting should produce 20-digit zero-padded filenames") {
    val version2 = 2L
    val version24 = 24L
    val version1000 = 1000L

    // Test f-interpolator (what we currently use)
    val formatted2 = f"$version2%020d.json"
    val formatted24 = f"$version24%020d.json"
    val formatted1000 = f"$version1000%020d.json"

    println(s"Version 2 formatted: $formatted2")
    println(s"Version 24 formatted: $formatted24")
    println(s"Version 1000 formatted: $formatted1000")

    // Expected format (Delta Lake compatible)
    formatted2 should be ("00000000000000000002.json")
    formatted24 should be ("00000000000000000024.json")
    formatted1000 should be ("00000000000000001000.json")

    // Test the ParallelOps pattern that creates wrong filenames
    val index = 0
    val parallelPattern2 = f"${version2}_$index%03d.json"
    val parallelPattern24 = f"${version24}_$index%03d.json"

    println(s"ParallelOps pattern for version 2: $parallelPattern2")
    println(s"ParallelOps pattern for version 24: $parallelPattern24")

    // This is the BUG - these patterns create files like 2_000.json
    parallelPattern2 should be ("2_000.json")
    parallelPattern24 should be ("24_000.json")
  }

  test("String.format with Locale.ROOT should produce locale-independent filenames") {
    val version2 = 2L
    val version24 = 24L

    // Use String.format with explicit Locale.ROOT
    val formatted2 = String.format(java.util.Locale.ROOT, "%020d.json", version2.asInstanceOf[AnyRef])
    val formatted24 = String.format(java.util.Locale.ROOT, "%020d.json", version24.asInstanceOf[AnyRef])

    println(s"Locale.ROOT formatted version 2: $formatted2")
    println(s"Locale.ROOT formatted version 24: $formatted24")

    formatted2 should be ("00000000000000000002.json")
    formatted24 should be ("00000000000000000024.json")
  }
}
