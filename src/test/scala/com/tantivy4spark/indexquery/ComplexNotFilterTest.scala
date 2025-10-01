package com.tantivy4spark.indexquery

import com.tantivy4spark.TestBase

class ComplexNotFilterTest extends TestBase {

  test("Complex NOT filter validation with comprehensive test data") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/complex_not_filter_test"

    // Comprehensive test data with all combinations of available/unavailable and various prices
    val testData = Seq(
      (1, "product-a", true, 100),  // available=true, price=100
      (2, "product-b", false, 100), // available=false, price=100
      (3, "product-c", true, 200),  // available=true, price=200
      (4, "product-d", false, 200), // available=false, price=200
      (5, "product-e", true, 50),   // available=true, price=50
      (6, "product-f", false, 50),  // available=false, price=50
      (7, "product-g", true, 150),  // available=true, price=150
      (8, "product-h", false, 150)  // available=false, price=150
    ).toDF("id", "name", "available", "price")

    println("=== Test Data ===")
    testData.show()

    // Write with default field types (no fast fields for price to test proper fallback)
    testData.write
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.name", "text")
      .mode("overwrite")
      .save(testPath)

    // Read and create temp view
    val df = spark.read
      .format("com.tantivy4spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("products")

    println("\n=== Complex NOT Filter Tests ===")

    // Test 1: NOT ((available = false) AND (price = 100))
    // This means: available = true OR price != 100
    // Expected: All records EXCEPT id=2 (available=false AND price=100)
    println("\n🔍 Test 1: NOT ((available = false) AND (price = 100))")
    val test1Results = spark
      .sql("""
      SELECT id, name, available, price FROM products
      WHERE NOT ((available = false) AND (price = 100))
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${test1Results.length}")
    test1Results.foreach(row =>
      println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, available=${row.getBoolean(2)}, price=${row.getInt(3)}")
    )

    val expected1 = Set(1, 3, 4, 5, 6, 7, 8) // All except id=2
    val actual1   = test1Results.map(_.getInt(0)).toSet
    assert(actual1 == expected1, s"Test 1: Expected $expected1 but got $actual1")

    // Validate the logic: should be (available=true OR price!=100)
    test1Results.foreach { row =>
      val available = row.getBoolean(2)
      val price     = row.getInt(3)
      assert(
        available || price != 100,
        s"Record should have available=true OR price!=100, but available=$available, price=$price"
      )
    }
    println("✅ Test 1 passed: NOT ((available = false) AND (price = 100))")

    // Test 2: NOT ((available = false) AND (price <> 100))
    // This means: available = true OR price = 100
    // Expected: Records where available=true OR price=100 (ids: 1, 2, 3, 5, 7)
    println("\n🔍 Test 2: NOT ((available = false) AND (price <> 100))")
    val test2Results = spark
      .sql("""
      SELECT id, name, available, price FROM products
      WHERE NOT ((available = false) AND (price <> 100))
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${test2Results.length}")
    test2Results.foreach(row =>
      println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, available=${row.getBoolean(2)}, price=${row.getInt(3)}")
    )

    val expected2 = Set(1, 2, 3, 5, 7) // available=true OR price=100
    val actual2   = test2Results.map(_.getInt(0)).toSet
    assert(actual2 == expected2, s"Test 2: Expected $expected2 but got $actual2")

    // Validate the logic: should be (available=true OR price=100)
    test2Results.foreach { row =>
      val available = row.getBoolean(2)
      val price     = row.getInt(3)
      assert(
        available || price == 100,
        s"Record should have available=true OR price=100, but available=$available, price=$price"
      )
    }
    println("✅ Test 2 passed: NOT ((available = false) AND (price <> 100))")

    // Test 3: More complex - NOT ((available = false) AND (price > 100))
    // This means: available = true OR price <= 100
    println("\n🔍 Test 3: NOT ((available = false) AND (price > 100))")
    val test3Results = spark
      .sql("""
      SELECT id, name, available, price FROM products
      WHERE NOT ((available = false) AND (price > 100))
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${test3Results.length}")
    test3Results.foreach(row =>
      println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, available=${row.getBoolean(2)}, price=${row.getInt(3)}")
    )

    val expected3 = Set(1, 2, 3, 5, 6, 7) // available=true OR price<=100
    val actual3   = test3Results.map(_.getInt(0)).toSet
    assert(actual3 == expected3, s"Test 3: Expected $expected3 but got $actual3")

    // Validate the logic: should be (available=true OR price<=100)
    test3Results.foreach { row =>
      val available = row.getBoolean(2)
      val price     = row.getInt(3)
      assert(
        available || price <= 100,
        s"Record should have available=true OR price<=100, but available=$available, price=$price"
      )
    }
    println("✅ Test 3 passed: NOT ((available = false) AND (price > 100))")

    println("\n🎉 All complex NOT filter tests completed successfully!")
    println("✅ Complex boolean logic with range queries works correctly")
    println("✅ Proper fallback to Spark filtering for unsupported range queries")
  }
}
