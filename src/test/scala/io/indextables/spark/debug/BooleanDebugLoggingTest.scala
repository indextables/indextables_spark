/*
 * Minimal test to debug boolean value conversion with forced logging
 */

package io.indextables.spark.debug

import io.indextables.spark.TestBase
import io.indextables.spark.core.FiltersToQueryConverter
import io.indextables.tantivy4java.core.{FieldType, SchemaBuilder}
import org.apache.spark.sql.sources.{EqualTo, Filter}
import org.scalatest.matchers.should.Matchers

/** Test to debug boolean value conversion with explicit logging */
class BooleanDebugLoggingTest extends TestBase with Matchers {

  test("debug boolean value conversion") {
    // Create a simple schema with boolean field
    val schemaBuilder = new SchemaBuilder()
    schemaBuilder.addBooleanField("active", true, true, true)
    val schema = schemaBuilder.build()

    try {
      // Test different boolean value types
      val testValues = Seq(
        ("Java Boolean true", java.lang.Boolean.TRUE),
        ("Java Boolean false", java.lang.Boolean.FALSE),
        ("Scala Boolean true", true),
        ("Scala Boolean false", false),
        ("Java boolean primitive", Boolean.box(true)),
        ("Java boolean primitive false", Boolean.box(false))
      )

      println("=== Boolean Value Conversion Debug ===")

      testValues.foreach {
        case (description, value) =>
          println(s"\nTesting: $description (${value.getClass.getSimpleName})")

          try {
            // Create EqualTo filter
            val filter = EqualTo("active", value)

            // Convert to query - this should trigger our debug logging
            val query = FiltersToQueryConverter.convertToQuery(Array[Filter](filter), schema)

            println(s"✅ Successfully created query: ${query.getClass.getSimpleName}")

            // Close query
            query.close()
          } catch {
            case e: Exception =>
              println(s"❌ Failed to create query: ${e.getMessage}")
          }
      }

      println("\n=== Testing with Invalid Boolean Types ===")

      val invalidValues = Seq(
        ("String true", "true"),
        ("String false", "false"),
        ("Integer 1", 1),
        ("Integer 0", 0),
        ("Long 1", 1L),
        ("Long 0", 0L)
      )

      invalidValues.foreach {
        case (description, value) =>
          println(s"\nTesting invalid: $description (${value.getClass.getSimpleName})")

          try {
            val filter = EqualTo("active", value)
            val query  = FiltersToQueryConverter.convertToQuery(Array[Filter](filter), schema)
            println(s"✅ Converted invalid value (should convert): ${query.getClass.getSimpleName}")
            query.close()
          } catch {
            case e: Exception =>
              println(s"❌ Rejected invalid value: ${e.getMessage}")
          }
      }

    } finally
      schema.close()
  }
}
