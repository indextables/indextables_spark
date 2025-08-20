#!/usr/bin/env python3
"""
Comprehensive PySpark integration tests for Tantivy4Spark.

This test suite validates the full functionality of Tantivy4Spark from Python,
including data writing, reading, querying, and all supported data types.
"""

import pytest
import tempfile
import shutil
import os
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


class TestTantivy4SparkIntegration:
    """Comprehensive integration tests for Tantivy4Spark with PySpark."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session with Tantivy4Spark shaded JAR for all tests."""
        import os
        
        # Get the shaded JAR path (adjust path as needed for your environment)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(current_dir, "..", "..", "..")
        shaded_jar_path = os.path.join(project_root, "target", "tantivy4spark-1.0.0-SNAPSHOT-shaded.jar")
        
        # Verify shaded JAR exists
        if not os.path.exists(shaded_jar_path):
            # Fallback to regular JAR if shaded not found
            regular_jar_path = os.path.join(project_root, "target", "tantivy4spark-1.0.0-SNAPSHOT.jar")
            if os.path.exists(regular_jar_path):
                jar_path = regular_jar_path
                print(f"Warning: Using regular JAR instead of shaded JAR: {jar_path}")
            else:
                raise FileNotFoundError(f"Neither shaded JAR nor regular JAR found. Expected: {shaded_jar_path}")
        else:
            jar_path = shaded_jar_path
            print(f"Using shaded JAR: {jar_path}")
        
        cls.spark = (SparkSession.builder
                    .appName("Tantivy4Spark-PySpark-Integration-Tests")
                    .config("spark.jars", jar_path)
                    .config("spark.sql.extensions", "com.tantivy4spark.Tantivy4SparkExtensions")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.sql.adaptive.skewJoin.enabled", "true")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .config("spark.driver.maxResultSize", "1g")
                    .getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")
        
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        cls.spark.stop()
    
    def setup_method(self):
        """Set up temporary directory for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_path = f"file://{self.temp_dir}/test_data"
        
    def teardown_method(self):
        """Clean up temporary directory after each test."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def test_basic_write_read_cycle(self):
        """Test basic write and read operations."""
        # Create test data
        data = [
            (1, "Alice", 25, 75000.50, True),
            (2, "Bob", 30, 85000.25, False),
            (3, "Charlie", 35, 95000.75, True)
        ]
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("active", BooleanType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write using Tantivy4Spark format
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        # Read back the data
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Verify data integrity
        assert read_df.count() == 3
        assert set(read_df.columns) == {"id", "name", "age", "salary", "active"}
        
        # Verify specific values
        alice = read_df.filter(col("name") == "Alice").collect()[0]
        assert alice["id"] == 1
        assert alice["age"] == 25
        assert alice["salary"] == 75000.50
        assert alice["active"] is True

    def test_comprehensive_data_types(self):
        """Test all supported data types."""
        from datetime import datetime, date
        
        # Create comprehensive test data
        data = [
            (
                1,                              # int
                12345678901234,                 # long  
                "Test String",                  # string
                3.14159,                        # double
                2.718,                          # float (will be converted to double)
                True,                           # boolean
                bytearray(b"binary data"),      # binary
                datetime(2023, 12, 25, 10, 30, 0),  # timestamp
                date(2023, 12, 25)             # date
            ),
            (
                2,
                98765432109876,
                "Another String",
                1.414,
                1.732,
                False,
                bytearray(b"more binary"),
                datetime(2023, 11, 15, 14, 45, 30),
                date(2023, 11, 15)
            )
        ]
        
        schema = StructType([
            StructField("int_col", IntegerType(), True),
            StructField("long_col", LongType(), True),
            StructField("string_col", StringType(), True),
            StructField("double_col", DoubleType(), True),
            StructField("float_col", FloatType(), True),
            StructField("bool_col", BooleanType(), True),
            StructField("binary_col", BinaryType(), True),
            StructField("timestamp_col", TimestampType(), True),
            StructField("date_col", DateType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write and read
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Verify all columns are present
        expected_columns = {
            "int_col", "long_col", "string_col", "double_col", 
            "float_col", "bool_col", "binary_col", "timestamp_col", "date_col"
        }
        assert set(read_df.columns) == expected_columns
        
        # Verify data integrity
        assert read_df.count() == 2
        
        # Test specific type conversions
        first_row = read_df.filter(col("int_col") == 1).collect()[0]
        assert first_row["string_col"] == "Test String"
        assert abs(first_row["double_col"] - 3.14159) < 0.0001
        assert first_row["bool_col"] is True

    def test_unsupported_data_types(self):
        """Test that unsupported data types are properly rejected."""
        
        # Test Array type rejection
        with pytest.raises(Exception) as exc_info:
            array_data = [(1, ["tag1", "tag2", "tag3"])]
            array_schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("tags", ArrayType(StringType()), True)
            ])
            array_df = self.spark.createDataFrame(array_data, array_schema)
            array_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_array")
        
        assert "Array types are not supported by Tantivy4Spark" in str(exc_info.value)
        
        # Test Map type rejection
        with pytest.raises(Exception) as exc_info:
            map_data = [(1, {"key1": "value1", "key2": "value2"})]
            map_schema = StructType([
                StructField("id", IntegerType(), False),
                StructField("metadata", MapType(StringType(), StringType()), True)
            ])
            map_df = self.spark.createDataFrame(map_data, map_schema)
            map_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_map")
        
        assert "Map types are not supported by Tantivy4Spark" in str(exc_info.value)

    def test_text_search_queries(self):
        """Test various text search operations."""
        # Create text-rich test data
        data = [
            (1, "Apache Spark Performance Guide", "This comprehensive guide covers Apache Spark optimization techniques", "technology", "John Smith"),
            (2, "Machine Learning with MLlib", "Learn machine learning algorithms using Spark MLlib framework", "AI", "Jane Doe"),
            (3, "Stream Processing Tutorial", "Real-time data processing with Spark Streaming and structured streaming", "streaming", "Bob Johnson"),
            (4, "Data Science Handbook", "Complete guide to data science with Python and Spark", "data-science", "Alice Brown"),
            (5, "Big Data Analytics", "Advanced analytics techniques for large-scale data processing", "analytics", "Charlie Davis")
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("author", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test contains operations
        spark_results = read_df.filter(col("title").contains("Spark")).collect()
        assert len(spark_results) >= 1
        assert any("Spark" in row["title"] for row in spark_results)
        
        # Test startsWith operations
        data_results = read_df.filter(col("title").startswith("Data")).collect()
        assert len(data_results) >= 1
        assert all(row["title"].startswith("Data") for row in data_results)
        
        # Test multiple text conditions
        ml_tech_results = read_df.filter(
            col("description").contains("machine learning") & 
            col("category").contains("AI")
        ).collect()
        assert len(ml_tech_results) >= 1
        
        # Test case sensitivity
        apache_results = read_df.filter(col("title").contains("Apache")).collect()
        assert len(apache_results) >= 1

    def test_numeric_range_queries(self):
        """Test numeric range queries and comparisons."""
        # Create numeric test data
        data = []
        for i in range(100):
            data.append((
                i,                              # id
                20 + (i % 50),                 # age (20-69)
                40000.0 + (i * 1000),         # salary (40k-139k)
                round(i * 0.1, 2),            # score (0.0-9.9)
                i % 10                         # priority (0-9)
            ))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("age", IntegerType(), True),
            StructField("salary", DoubleType(), True),
            StructField("score", DoubleType(), True),
            StructField("priority", IntegerType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test greater than
        high_salary = read_df.filter(col("salary") > 80000).collect()
        assert len(high_salary) > 0
        assert all(row["salary"] > 80000 for row in high_salary)
        
        # Test less than
        young_employees = read_df.filter(col("age") < 30).collect()
        assert len(young_employees) > 0
        assert all(row["age"] < 30 for row in young_employees)
        
        # Test between (range)
        mid_range = read_df.filter(col("salary").between(60000, 100000)).collect()
        assert len(mid_range) > 0
        assert all(60000 <= row["salary"] <= 100000 for row in mid_range)
        
        # Test exact equality
        specific_priority = read_df.filter(col("priority") == 5).collect()
        assert len(specific_priority) > 0
        assert all(row["priority"] == 5 for row in specific_priority)
        
        # Test IN clause
        priority_in = read_df.filter(col("priority").isin([1, 3, 5, 7, 9])).collect()
        assert len(priority_in) > 0
        assert all(row["priority"] in [1, 3, 5, 7, 9] for row in priority_in)

    def test_boolean_and_null_handling(self):
        """Test boolean operations and null value handling."""
        # Create data with nulls and booleans
        data = [
            (1, "Alice", True, None, 25),
            (2, "Bob", False, "Manager", 30),
            (3, "Charlie", True, "Developer", None),
            (4, "Diana", None, "Analyst", 28),
            (5, "Eve", False, None, 35)
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("active", BooleanType(), True),
            StructField("role", StringType(), True),
            StructField("age", IntegerType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test boolean filtering
        active_users = read_df.filter(col("active") == True).collect()
        assert len(active_users) >= 2
        assert all(row["active"] is True for row in active_users if row["active"] is not None)
        
        inactive_users = read_df.filter(col("active") == False).collect()
        assert len(inactive_users) >= 2
        assert all(row["active"] is False for row in inactive_users if row["active"] is not None)
        
        # Test null filtering
        null_roles = read_df.filter(col("role").isNull()).collect()
        assert len(null_roles) >= 2
        assert all(row["role"] is None for row in null_roles)
        
        not_null_roles = read_df.filter(col("role").isNotNull()).collect()
        assert len(not_null_roles) >= 2
        assert all(row["role"] is not None for row in not_null_roles)
        
        # Test combined boolean and null conditions
        active_with_role = read_df.filter(
            (col("active") == True) & col("role").isNotNull()
        ).collect()
        assert len(active_with_role) >= 1

    def test_aggregation_operations(self):
        """Test aggregation functions with Tantivy4Spark."""
        # Create aggregation test data
        departments = ["Engineering", "Marketing", "Sales", "HR", "Finance"]
        locations = ["New York", "San Francisco", "Chicago", "Austin"]
        
        data = []
        for i in range(50):
            data.append((
                i,
                f"Employee_{i}",
                departments[i % len(departments)],
                locations[i % len(locations)],
                50000 + (i * 2000),
                25 + (i % 40),
                i % 2 == 0
            ))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("department", StringType(), True),
            StructField("location", StringType(), True),
            StructField("salary", IntegerType(), True),
            StructField("age", IntegerType(), True),
            StructField("active", BooleanType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test basic aggregations
        total_count = read_df.count()
        assert total_count == 50
        
        # Test group by aggregations
        dept_stats = read_df.groupBy("department").agg(
            count("*").alias("employee_count"),
            avg("salary").alias("avg_salary"),
            max("age").alias("max_age"),
            min("age").alias("min_age")
        ).collect()
        
        assert len(dept_stats) == len(departments)
        for row in dept_stats:
            assert row["employee_count"] > 0
            assert row["avg_salary"] > 0
            assert row["max_age"] >= row["min_age"]
        
        # Test filtered aggregations
        engineering_avg = read_df.filter(col("department") == "Engineering").agg(
            avg("salary").alias("avg_eng_salary")
        ).collect()[0]["avg_eng_salary"]
        
        assert engineering_avg is not None
        assert engineering_avg > 0

    def test_complex_compound_queries(self):
        """Test complex queries with multiple conditions."""
        # Create comprehensive test data
        data = []
        for i in range(200):
            data.append((
                i,
                f"Product_{i}",
                f"Description for product {i} with various features",
                ["Electronics", "Books", "Clothing", "Home", "Sports"][i % 5],
                round(10.0 + (i * 5.5), 2),
                100 + (i * 10),
                i % 3 == 0,  # available
                ["Low", "Medium", "High", "Premium"][i % 4],
                f"Brand_{i % 10}"
            ))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("description", StringType(), True),
            StructField("category", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("inventory", IntegerType(), True),
            StructField("available", BooleanType(), True),
            StructField("tier", StringType(), True),
            StructField("brand", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Complex query 1: Multiple AND conditions
        complex_filter_1 = read_df.filter(
            (col("category") == "Electronics") &
            (col("price") > 100.0) &
            (col("available") == True) &
            (col("inventory") > 500)
        ).collect()
        
        for row in complex_filter_1:
            assert row["category"] == "Electronics"
            assert row["price"] > 100.0
            assert row["available"] is True
            assert row["inventory"] > 500
        
        # Complex query 2: OR and AND combinations
        complex_filter_2 = read_df.filter(
            ((col("category") == "Books") | (col("category") == "Electronics")) &
            (col("tier").isin(["High", "Premium"])) &
            (col("price").between(50.0, 500.0))
        ).collect()
        
        for row in complex_filter_2:
            assert row["category"] in ["Books", "Electronics"]
            assert row["tier"] in ["High", "Premium"]
            assert 50.0 <= row["price"] <= 500.0
        
        # Complex query 3: Text search with other conditions
        text_and_numeric = read_df.filter(
            col("description").contains("product") &
            (col("price") < 200.0) &
            col("name").startswith("Product_1")
        ).collect()
        
        for row in text_and_numeric:
            assert "product" in row["description"].lower()
            assert row["price"] < 200.0
            assert row["name"].startswith("Product_1")

    def test_partitioned_data_operations(self):
        """Test operations on partitioned datasets."""
        # Create partitioned test data
        data = []
        years = [2021, 2022, 2023]
        months = list(range(1, 13))
        
        for year in years:
            for month in months:
                for day in range(1, 11):  # 10 days per month
                    data.append((
                        len(data) + 1,
                        f"Event_{len(data) + 1}",
                        year,
                        month,
                        f"{year}-{month:02d}-{day:02d}",
                        round(100.0 + (len(data) * 1.5), 2)
                    ))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("event_name", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("date_str", StringType(), True),
            StructField("value", DoubleType(), True)
        ])
        
        df = self.spark.createDataFrame(data, schema)
        
        # Write with partitioning
        df.write.format("tantivy4spark").mode("overwrite").partitionBy("year", "month").save(self.test_path)
        
        # Read partitioned data
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test partition pruning
        year_2022 = read_df.filter(col("year") == 2022).collect()
        assert len(year_2022) > 0
        assert all(row["year"] == 2022 for row in year_2022)
        
        # Test multiple partition filters
        specific_month = read_df.filter(
            (col("year") == 2023) & (col("month") == 6)
        ).collect()
        assert len(specific_month) > 0
        assert all(row["year"] == 2023 and row["month"] == 6 for row in specific_month)
        
        # Test aggregations on partitioned data
        yearly_stats = read_df.groupBy("year").agg(
            count("*").alias("event_count"),
            avg("value").alias("avg_value")
        ).orderBy("year").collect()
        
        assert len(yearly_stats) == len(years)
        for i, row in enumerate(yearly_stats):
            assert row["year"] == years[i]
            assert row["event_count"] > 0

    def test_schema_evolution_scenarios(self):
        """Test schema evolution and compatibility."""
        # Initial schema and data
        initial_data = [
            (1, "Product A", 19.99),
            (2, "Product B", 29.99),
            (3, "Product C", 39.99)
        ]
        
        initial_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True)
        ])
        
        initial_df = self.spark.createDataFrame(initial_data, initial_schema)
        initial_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        # Verify initial write
        read_initial = self.spark.read.format("tantivy4spark").load(self.test_path)
        assert read_initial.count() == 3
        assert set(read_initial.columns) == {"id", "name", "price"}
        
        # Evolved schema with additional columns
        evolved_data = [
            (4, "Product D", 49.99, "Electronics", True, "2023-12-01"),
            (5, "Product E", 59.99, "Books", False, "2023-12-02")
        ]
        
        evolved_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("price", DoubleType(), True),
            StructField("category", StringType(), True),
            StructField("featured", BooleanType(), True),
            StructField("launch_date", StringType(), True)
        ])
        
        evolved_df = self.spark.createDataFrame(evolved_data, evolved_schema)
        
        # Append evolved data (simulating schema evolution)
        evolved_df.write.format("tantivy4spark").mode("append").save(self.test_path)
        
        # Read evolved dataset
        read_evolved = self.spark.read.format("tantivy4spark").load(self.test_path)
        assert read_evolved.count() == 5
        
        # Verify that new columns are accessible
        evolved_columns = set(read_evolved.columns)
        expected_columns = {"id", "name", "price", "category", "featured", "launch_date"}
        assert expected_columns.issubset(evolved_columns)

    def test_error_handling_and_edge_cases(self):
        """Test error handling and edge cases."""
        
        # Test empty dataset
        empty_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("value", StringType(), True)
        ])
        empty_df = self.spark.createDataFrame([], empty_schema)
        empty_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_empty")
        
        read_empty = self.spark.read.format("tantivy4spark").load(self.test_path + "_empty")
        assert read_empty.count() == 0
        assert set(read_empty.columns) == {"id", "value"}
        
        # Test single row dataset
        single_data = [(1, "single_value")]
        single_df = self.spark.createDataFrame(single_data, empty_schema)
        single_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_single")
        
        read_single = self.spark.read.format("tantivy4spark").load(self.test_path + "_single")
        assert read_single.count() == 1
        assert read_single.collect()[0]["value"] == "single_value"
        
        # Test with special characters and Unicode
        unicode_data = [
            (1, "Hello 世界", "Special chars: !@#$%^&*()"),
            (2, "Café naïve résumé", "Émojis: 🚀🎉💻"),
            (3, "Русский текст", "العربية")
        ]
        
        unicode_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("description", StringType(), True)
        ])
        
        unicode_df = self.spark.createDataFrame(unicode_data, unicode_schema)
        unicode_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_unicode")
        
        read_unicode = self.spark.read.format("tantivy4spark").load(self.test_path + "_unicode")
        assert read_unicode.count() == 3
        
        # Verify Unicode preservation
        unicode_results = read_unicode.collect()
        assert any("世界" in row["title"] for row in unicode_results)
        assert any("🚀" in row["description"] for row in unicode_results)

    def test_performance_with_large_dataset(self):
        """Test performance characteristics with larger datasets."""
        # Create a larger dataset for performance testing
        large_data = []
        for i in range(1000):
            large_data.append((
                i,
                f"Document_{i}",
                f"This is the content of document number {i} with some searchable text about technology, data science, and machine learning.",
                f"Category_{i % 10}",
                f"Author_{i % 50}",
                round(100.0 + (i * 0.5), 2),
                i % 2 == 0
            ))
        
        large_schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("category", StringType(), True),
            StructField("author", StringType(), True),
            StructField("score", DoubleType(), True),
            StructField("published", BooleanType(), True)
        ])
        
        large_df = self.spark.createDataFrame(large_data, large_schema)
        
        # Measure write performance
        import time
        start_time = time.time()
        large_df.write.format("tantivy4spark").mode("overwrite").save(self.test_path + "_large")
        write_time = time.time() - start_time
        
        print(f"Write time for 1000 records: {write_time:.2f} seconds")
        
        # Measure read performance
        start_time = time.time()
        read_large = self.spark.read.format("tantivy4spark").load(self.test_path + "_large")
        total_count = read_large.count()
        read_time = time.time() - start_time
        
        print(f"Read time for {total_count} records: {read_time:.2f} seconds")
        
        # Verify data integrity
        assert total_count == 1000
        
        # Test search performance on large dataset
        start_time = time.time()
        search_results = read_large.filter(
            col("content").contains("technology") & 
            (col("score") > 200.0)
        ).collect()
        search_time = time.time() - start_time
        
        print(f"Search time: {search_time:.2f} seconds, Results: {len(search_results)}")
        assert len(search_results) > 0

    def test_append_mode_operations(self):
        """Test append mode and incremental data loading."""
        # Initial batch
        batch1 = [
            (1, "Initial Document 1", "First batch content"),
            (2, "Initial Document 2", "More first batch content")
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True)
        ])
        
        df1 = self.spark.createDataFrame(batch1, schema)
        df1.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        # Verify initial batch
        read_after_batch1 = self.spark.read.format("tantivy4spark").load(self.test_path)
        assert read_after_batch1.count() == 2
        
        # Second batch (append)
        batch2 = [
            (3, "Appended Document 1", "Second batch content"),
            (4, "Appended Document 2", "More second batch content"),
            (5, "Appended Document 3", "Even more content")
        ]
        
        df2 = self.spark.createDataFrame(batch2, schema)
        df2.write.format("tantivy4spark").mode("append").save(self.test_path)
        
        # Verify combined data
        read_after_batch2 = self.spark.read.format("tantivy4spark").load(self.test_path)
        assert read_after_batch2.count() == 5
        
        # Verify all data is accessible
        all_ids = [row["id"] for row in read_after_batch2.collect()]
        assert sorted(all_ids) == [1, 2, 3, 4, 5]
        
        # Test queries across both batches
        first_batch_results = read_after_batch2.filter(col("content").contains("first batch")).collect()
        second_batch_results = read_after_batch2.filter(col("content").contains("second batch")).collect()
        
        assert len(first_batch_results) == 2
        assert len(second_batch_results) == 2


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])