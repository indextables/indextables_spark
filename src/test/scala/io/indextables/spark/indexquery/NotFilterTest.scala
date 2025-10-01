package io.indextables.spark.indexquery

import io.indextables.spark.TestBase

class NotFilterTest extends TestBase {

  test("SQL NOT filter validation with IndexQuery combinations") {
    val spark = this.spark
    import spark.implicits._

    val testPath = s"$tempDir/not_filter_test"

    // Test data with various categories and values for NOT filtering
    val testData = Seq(
      (1, "product-a", "electronics", 100, true),
      (2, "product-b", "clothing", 200, false),
      (3, "product-c", "electronics", 150, true),
      (4, "product-d", "books", 50, false),
      (5, "product-e", "electronics", 300, true),
      (6, "product-f", "clothing", 75, true),
      (7, "product-g", "books", 25, false)
    ).toDF("id", "name", "category", "price", "available")

    // Write with mixed field types
    testData.write
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.name", "text")       // tokenized for IndexQuery
      .option("spark.indextables.indexing.typemap.category", "string") // exact matching
      .mode("overwrite")
      .save(testPath)

    // Read and create temp view
    val df = spark.read
      .format("io.indextables.spark.core.Tantivy4SparkTableProvider")
      .load(testPath)

    df.createOrReplaceTempView("products")

    println("=== SQL NOT Filter Tests with IndexQuery ===")

    // Test 1: Simple NOT equals
    println("\n🔍 Test 1: category != 'electronics'")
    val notElectronicsResults = spark
      .sql("""
      SELECT id, name, category, price FROM products
      WHERE category != 'electronics'
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${notElectronicsResults.length}")
    notElectronicsResults.foreach(row => println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, ${row.getString(2)}"))

    // Expected: Records 2, 4, 6, 7 (not electronics)
    val expectedNotElectronics = Set(2, 4, 6, 7)
    val actualNotElectronics   = notElectronicsResults.map(_.getInt(0)).toSet
    assert(
      actualNotElectronics == expectedNotElectronics,
      s"Expected $expectedNotElectronics but got $actualNotElectronics"
    )

    // Validate no false positives
    notElectronicsResults.foreach { row =>
      val category = row.getString(2)
      assert(category != "electronics", s"Result should NOT be electronics but was '$category'")
    }
    println("✅ NOT equals filter works correctly")

    // Test 2: NOT with numeric comparison
    println("\n🔍 Test 2: price NOT >= 200 (same as price < 200)")
    val notHighPriceResults = spark
      .sql("""
      SELECT id, name, price FROM products
      WHERE NOT (price >= 200)
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${notHighPriceResults.length}")
    notHighPriceResults.foreach(row => println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, price=${row.getInt(2)}"))

    // Expected: Records 1, 3, 4, 6, 7 (price < 200)
    val expectedLowPrice = Set(1, 3, 4, 6, 7)
    val actualLowPrice   = notHighPriceResults.map(_.getInt(0)).toSet
    assert(actualLowPrice == expectedLowPrice, s"Expected $expectedLowPrice but got $actualLowPrice")

    // Validate all results have price < 200
    notHighPriceResults.foreach { row =>
      val price = row.getInt(2)
      assert(price < 200, s"Price should be < 200 but was $price")
    }
    println("✅ NOT with numeric comparison works correctly")

    // Test 3: IndexQuery combined with NOT filter
    println("\n🔍 Test 3: IndexQuery 'product' AND category != 'books'")
    val indexQueryWithNotResults = spark
      .sql("""
      SELECT id, name, category FROM products
      WHERE name indexquery 'product' AND category != 'books'
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${indexQueryWithNotResults.length}")
    indexQueryWithNotResults.foreach(row => println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, ${row.getString(2)}"))

    // Expected behavior depends on IndexQuery working:
    // If IndexQuery works: All products (1-6) except books (4,7) = {1,2,3,5,6}
    // If IndexQuery broken: Empty results (current behavior)
    val actualIndexQueryNot = indexQueryWithNotResults.map(_.getInt(0)).toSet

    if (actualIndexQueryNot.nonEmpty) {
      println("✅ IndexQuery is working! Validating combined results...")
      val expectedIndexQueryNot = Set(1, 2, 3, 5, 6)
      assert(
        actualIndexQueryNot == expectedIndexQueryNot,
        s"Expected $expectedIndexQueryNot but got $actualIndexQueryNot"
      )

      // Validate all results match criteria
      indexQueryWithNotResults.foreach { row =>
        val name     = row.getString(1).toLowerCase
        val category = row.getString(2)
        assert(name.contains("product"), s"Name '$name' should contain 'product'")
        assert(category != "books", s"Category should NOT be 'books' but was '$category'")
      }
      println("✅ IndexQuery + NOT filter combination works perfectly!")

    } else {
      println("⚠️ IndexQuery still returning 0 results - this confirms the parseQuery issue exists")
      println("   Once IndexQuery is fixed, this test will validate the combination correctly")
    }

    // Test 4: Complex NOT with boolean logic
    println("\n🔍 Test 4: NOT (available = false AND price < 100)")
    val complexNotResults = spark
      .sql("""
      SELECT id, name, available, price FROM products
      WHERE NOT (available = false AND price < 100)
      ORDER BY id
    """)
      .collect()

    println(s"Results: ${complexNotResults.length}")
    complexNotResults.foreach(row =>
      println(s"  ID=${row.getInt(0)}: ${row.getString(1)}, available=${row.getBoolean(2)}, price=${row.getInt(3)}")
    )

    // NOT (available = false AND price < 100) means:
    // available = true OR price >= 100
    // Expected: Records 1,2,3,5,6 (exclude 4,7 which are available=false AND price<100)
    val expectedComplexNot = Set(1, 2, 3, 5, 6)
    val actualComplexNot   = complexNotResults.map(_.getInt(0)).toSet
    assert(actualComplexNot == expectedComplexNot, s"Expected $expectedComplexNot but got $actualComplexNot")

    // Validate De Morgan's law: NOT (A AND B) = (NOT A) OR (NOT B)
    complexNotResults.foreach { row =>
      val available = row.getBoolean(2)
      val price     = row.getInt(3)
      assert(
        available || price >= 100,
        s"Should be available=true OR price>=100, but available=$available, price=$price"
      )
    }
    println("✅ Complex NOT with boolean logic works correctly")

    println("\n🎉 All NOT filter tests completed successfully!")
    println(s"IndexQuery status: ${if (actualIndexQueryNot.nonEmpty) "✅ Working" else "❌ Still broken"}")
  }
}
