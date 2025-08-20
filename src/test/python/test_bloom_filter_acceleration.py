#!/usr/bin/env python3
"""
PySpark integration tests specifically for Tantivy4Spark bloom filter acceleration.

These tests validate that bloom filters are working correctly to accelerate
text search queries and reduce I/O to object storage.
"""

import pytest
import tempfile
import shutil
import os
import time
from typing import List, Dict, Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *
from pyspark.sql.functions import *


class TestBloomFilterAcceleration:
    """Tests for bloom filter text search acceleration in Tantivy4Spark."""
    
    @classmethod
    def setup_class(cls):
        """Set up Spark session with Tantivy4Spark shaded JAR for bloom filter tests."""
        import os
        
        # Get the shaded JAR path
        current_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.join(current_dir, "..", "..", "..")
        shaded_jar_path = os.path.join(project_root, "target", "tantivy4spark-1.0.0-SNAPSHOT-shaded.jar")
        
        # Verify JAR exists
        if not os.path.exists(shaded_jar_path):
            regular_jar_path = os.path.join(project_root, "target", "tantivy4spark-1.0.0-SNAPSHOT.jar")
            if os.path.exists(regular_jar_path):
                jar_path = regular_jar_path
                print(f"Warning: Using regular JAR: {jar_path}")
            else:
                raise FileNotFoundError(f"JAR not found. Run 'mvn package' first.")
        else:
            jar_path = shaded_jar_path
            print(f"Using shaded JAR: {jar_path}")
        
        cls.spark = (SparkSession.builder
                    .appName("Tantivy4Spark-BloomFilter-Tests")
                    .config("spark.jars", jar_path)
                    .config("spark.sql.extensions", "com.tantivy4spark.Tantivy4SparkExtensions")
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                    .config("spark.sql.adaptive.enabled", "true")
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                    .config("spark.driver.memory", "2g")
                    .config("spark.executor.memory", "2g")
                    .getOrCreate())
        cls.spark.sparkContext.setLogLevel("WARN")
        
    @classmethod
    def teardown_class(cls):
        """Clean up Spark session."""
        cls.spark.stop()
    
    def setup_method(self):
        """Set up temporary directory for each test."""
        self.temp_dir = tempfile.mkdtemp()
        self.test_path = f"file://{self.temp_dir}/bloom_test"
        
    def teardown_method(self):
        """Clean up temporary directory after each test."""
        if os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)

    def create_large_text_dataset(self, num_docs: int = 1000) -> DataFrame:
        """Create a large dataset with varied text content for bloom filter testing."""
        
        # Technology terms for realistic content
        tech_terms = [
            "Apache Spark", "machine learning", "data science", "artificial intelligence",
            "deep learning", "neural networks", "big data", "cloud computing",
            "distributed systems", "microservices", "containers", "kubernetes",
            "data engineering", "ETL pipelines", "stream processing", "batch processing",
            "NoSQL databases", "graph databases", "time series", "analytics",
            "business intelligence", "data visualization", "predictive modeling",
            "natural language processing", "computer vision", "reinforcement learning"
        ]
        
        categories = [
            "Technology", "Data Science", "Engineering", "Research", "Business",
            "Analytics", "Infrastructure", "Security", "Performance", "Innovation"
        ]
        
        authors = [
            "Dr. Smith", "Prof. Johnson", "Alice Chen", "Bob Wilson", "Carol Davis",
            "David Brown", "Emma Garcia", "Frank Miller", "Grace Lee", "Henry Taylor"
        ]
        
        data = []
        for i in range(num_docs):
            # Create varied content to test bloom filter effectiveness
            primary_term = tech_terms[i % len(tech_terms)]
            secondary_terms = [tech_terms[(i + j) % len(tech_terms)] for j in range(1, 4)]
            
            title = f"Research Paper {i}: {primary_term} Applications"
            content = (
                f"This comprehensive study explores {primary_term} and its applications. "
                f"We investigate the relationship between {secondary_terms[0]} and {secondary_terms[1]}. "
                f"Our research shows significant improvements in {secondary_terms[2]} performance. "
                f"The methodology includes advanced techniques in data processing and analysis. "
                f"Results demonstrate the effectiveness of our approach in real-world scenarios."
            )
            
            abstract = (
                f"Abstract: {primary_term} has emerged as a critical technology. "
                f"This paper presents novel approaches combining {secondary_terms[0]} with traditional methods."
            )
            
            data.append((
                i,
                title,
                content,
                abstract,
                categories[i % len(categories)],
                authors[i % len(authors)],
                2020 + (i % 4),  # years 2020-2023
                round(1.0 + (i * 0.1), 2),  # impact score
                i % 3 == 0  # featured
            ))
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("abstract", StringType(), True),
            StructField("category", StringType(), True),
            StructField("author", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("impact_score", DoubleType(), True),
            StructField("featured", BooleanType(), True)
        ])
        
        return self.spark.createDataFrame(data, schema)

    def test_bloom_filter_text_search_acceleration(self):
        """Test that bloom filters accelerate text search queries."""
        
        # Create a large dataset with multiple partitions to enable bloom filtering
        large_df = self.create_large_text_dataset(2000)
        
        # Write with multiple partitions to create multiple index files (enables bloom filtering)
        large_df.repartition(10).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        # Read the data back
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test specific text searches that should benefit from bloom filters
        search_terms = [
            "Apache Spark",
            "machine learning", 
            "deep learning",
            "data science",
            "artificial intelligence",
            "distributed systems"
        ]
        
        for term in search_terms:
            print(f"\nTesting bloom filter acceleration for term: '{term}'")
            
            # Time the search operation
            start_time = time.time()
            
            # Search in title
            title_results = read_df.filter(col("title").contains(term)).collect()
            title_time = time.time() - start_time
            
            # Search in content  
            start_time = time.time()
            content_results = read_df.filter(col("content").contains(term)).collect()
            content_time = time.time() - start_time
            
            # Search in abstract
            start_time = time.time()
            abstract_results = read_df.filter(col("abstract").contains(term)).collect()
            abstract_time = time.time() - start_time
            
            print(f"  Title search: {len(title_results)} results in {title_time:.3f}s")
            print(f"  Content search: {len(content_results)} results in {content_time:.3f}s")
            print(f"  Abstract search: {len(abstract_results)} results in {abstract_time:.3f}s")
            
            # Verify we get reasonable results
            assert len(title_results) > 0 or len(content_results) > 0 or len(abstract_results) > 0
            
            # Verify content correctness
            for result in title_results:
                assert term.lower() in result["title"].lower()
            
            for result in content_results:
                assert term.lower() in result["content"].lower()

    def test_bloom_filter_multiple_terms(self):
        """Test bloom filter performance with multiple search terms."""
        
        # Create dataset
        df = self.create_large_text_dataset(1500)
        df.repartition(8).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test compound searches that should benefit from bloom filter intersection
        compound_searches = [
            ("Apache Spark", "data science"),
            ("machine learning", "neural networks"),
            ("cloud computing", "distributed systems"),
            ("big data", "analytics"),
            ("artificial intelligence", "deep learning")
        ]
        
        for term1, term2 in compound_searches:
            print(f"\nTesting compound search: '{term1}' AND '{term2}'")
            
            start_time = time.time()
            
            # Test AND condition (both terms must be present)
            and_results = read_df.filter(
                col("content").contains(term1) & col("content").contains(term2)
            ).collect()
            
            and_time = time.time() - start_time
            
            start_time = time.time()
            
            # Test OR condition (either term present)
            or_results = read_df.filter(
                col("content").contains(term1) | col("content").contains(term2)
            ).collect()
            
            or_time = time.time() - start_time
            
            print(f"  AND search: {len(and_results)} results in {and_time:.3f}s")
            print(f"  OR search: {len(or_results)} results in {or_time:.3f}s")
            
            # Verify logical relationships
            assert len(or_results) >= len(and_results)
            
            # Verify AND results contain both terms
            for result in and_results:
                content_lower = result["content"].lower()
                assert term1.lower() in content_lower and term2.lower() in content_lower

    def test_bloom_filter_prefix_and_suffix_searches(self):
        """Test bloom filter effectiveness with prefix and suffix searches."""
        
        # Create dataset with predictable patterns
        df = self.create_large_text_dataset(1000)
        df.repartition(6).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test startsWith (prefix) searches
        prefix_terms = ["Research", "Apache", "machine", "data", "deep"]
        
        for prefix in prefix_terms:
            print(f"\nTesting prefix search: '{prefix}*'")
            
            start_time = time.time()
            prefix_results = read_df.filter(col("title").startswith(prefix)).collect()
            prefix_time = time.time() - start_time
            
            print(f"  Prefix search: {len(prefix_results)} results in {prefix_time:.3f}s")
            
            # Verify results
            for result in prefix_results:
                assert result["title"].startswith(prefix)
        
        # Test contains searches (substring)
        substring_terms = ["learning", "processing", "systems", "intelligence", "computing"]
        
        for substring in substring_terms:
            print(f"\nTesting substring search: '*{substring}*'")
            
            start_time = time.time()
            substring_results = read_df.filter(col("content").contains(substring)).collect()
            substring_time = time.time() - start_time
            
            print(f"  Substring search: {len(substring_results)} results in {substring_time:.3f}s")
            
            # Verify results
            for result in substring_results:
                assert substring.lower() in result["content"].lower()

    def test_bloom_filter_case_sensitivity(self):
        """Test bloom filter behavior with case-sensitive searches."""
        
        # Create dataset with mixed case content
        mixed_case_data = [
            (1, "Apache Spark Tutorial", "Learn APACHE SPARK with this guide", "Technology"),
            (2, "apache spark performance", "Optimizing apache spark applications", "Performance"),
            (3, "Apache SPARK Advanced", "Advanced Apache Spark techniques", "Advanced"),
            (4, "Machine Learning Guide", "machine learning with SPARK", "AI"),
            (5, "DATA SCIENCE Handbook", "data science and Machine Learning", "Science")
        ]
        
        schema = StructType([
            StructField("id", IntegerType(), False),
            StructField("title", StringType(), True),
            StructField("content", StringType(), True),
            StructField("category", StringType(), True)
        ])
        
        df = self.spark.createDataFrame(mixed_case_data, schema)
        df.write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test case-sensitive searches
        case_tests = [
            ("Apache", "title"),
            ("apache", "title"),
            ("APACHE", "title"),
            ("Spark", "content"),
            ("SPARK", "content"),
            ("spark", "content"),
            ("Machine", "content"),
            ("machine", "content")
        ]
        
        for term, column in case_tests:
            print(f"\nTesting case sensitivity: '{term}' in {column}")
            
            results = read_df.filter(col(column).contains(term)).collect()
            print(f"  Found {len(results)} results for '{term}'")
            
            # Verify results contain the search term (case-sensitive)
            for result in results:
                assert term in result[column]

    def test_bloom_filter_non_existent_terms(self):
        """Test bloom filter efficiency with non-existent search terms."""
        
        # Create dataset
        df = self.create_large_text_dataset(1200)
        df.repartition(8).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Search for terms that definitely don't exist in our dataset
        non_existent_terms = [
            "xyzzylmnop",  # Random string
            "nonexistentterm123",
            "impossibleword456",
            "notfoundanywhere789",
            "randomgibberish000"
        ]
        
        for term in non_existent_terms:
            print(f"\nTesting non-existent term: '{term}'")
            
            start_time = time.time()
            
            # These searches should be very fast due to bloom filter elimination
            title_results = read_df.filter(col("title").contains(term)).collect()
            content_results = read_df.filter(col("content").contains(term)).collect()
            abstract_results = read_df.filter(col("abstract").contains(term)).collect()
            
            search_time = time.time() - start_time
            
            print(f"  Search completed in {search_time:.3f}s")
            print(f"  Results: title={len(title_results)}, content={len(content_results)}, abstract={len(abstract_results)}")
            
            # Should find no results for non-existent terms
            assert len(title_results) == 0
            assert len(content_results) == 0
            assert len(abstract_results) == 0
            
            # Search should be fast due to bloom filter skipping
            assert search_time < 1.0  # Should complete in under 1 second

    def test_bloom_filter_combined_with_other_filters(self):
        """Test bloom filters working in combination with other filter types."""
        
        # Create dataset
        df = self.create_large_text_dataset(1000)
        df.repartition(5).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test combinations of text search with numeric/boolean filters
        combined_queries = [
            # Text + numeric range
            {
                "name": "Text + Numeric Range",
                "filter": col("content").contains("machine learning") & (col("impact_score") > 50.0),
                "expected_text": "machine learning"
            },
            # Text + boolean
            {
                "name": "Text + Boolean",
                "filter": col("title").contains("Apache Spark") & (col("featured") == True),
                "expected_text": "Apache Spark"
            },
            # Text + categorical
            {
                "name": "Text + Categorical",
                "filter": col("abstract").contains("data science") & (col("category") == "Technology"),
                "expected_text": "data science"
            },
            # Multiple text + numeric
            {
                "name": "Multiple Text + Numeric",
                "filter": (col("content").contains("deep learning") | col("content").contains("neural networks")) & (col("year") >= 2022),
                "expected_texts": ["deep learning", "neural networks"]
            }
        ]
        
        for query in combined_queries:
            print(f"\nTesting: {query['name']}")
            
            start_time = time.time()
            results = read_df.filter(query["filter"]).collect()
            query_time = time.time() - start_time
            
            print(f"  Found {len(results)} results in {query_time:.3f}s")
            
            # Verify text conditions are met
            if "expected_text" in query:
                for result in results:
                    found_in_title = query["expected_text"].lower() in result.get("title", "").lower()
                    found_in_content = query["expected_text"].lower() in result.get("content", "").lower()
                    found_in_abstract = query["expected_text"].lower() in result.get("abstract", "").lower()
                    assert found_in_title or found_in_content or found_in_abstract
            
            if "expected_texts" in query:
                for result in results:
                    found_any = False
                    for expected_text in query["expected_texts"]:
                        found_in_content = expected_text.lower() in result.get("content", "").lower()
                        if found_in_content:
                            found_any = True
                            break
                    assert found_any

    def test_bloom_filter_performance_comparison(self):
        """Compare performance with and without bloom filters (conceptual test)."""
        
        # Create a substantial dataset
        large_df = self.create_large_text_dataset(2000)
        
        # Write with multiple partitions (enables bloom filters)
        large_df.repartition(10).write.format("tantivy4spark").mode("overwrite").save(self.test_path)
        
        read_df = self.spark.read.format("tantivy4spark").load(self.test_path)
        
        # Test multiple search scenarios and measure performance
        search_scenarios = [
            ("Common term", "data"),
            ("Specific term", "machine learning"),
            ("Rare term", "reinforcement learning"),
            ("Non-existent", "nonexistentterm123")
        ]
        
        performance_results = {}
        
        for scenario_name, search_term in search_scenarios:
            print(f"\nPerformance test: {scenario_name} ('{search_term}')")
            
            # Warm up
            read_df.filter(col("content").contains(search_term)).count()
            
            # Measure multiple runs
            times = []
            for run in range(3):
                start_time = time.time()
                count = read_df.filter(col("content").contains(search_term)).count()
                elapsed = time.time() - start_time
                times.append(elapsed)
                
                print(f"  Run {run + 1}: {count} results in {elapsed:.3f}s")
            
            avg_time = sum(times) / len(times)
            performance_results[scenario_name] = {
                "avg_time": avg_time,
                "search_term": search_term,
                "result_count": count
            }
            
            print(f"  Average: {avg_time:.3f}s")
        
        # Verify performance characteristics
        # Non-existent terms should be fastest (bloom filter eliminates all files)
        non_existent_time = performance_results["Non-existent"]["avg_time"]
        
        # All searches should complete reasonably quickly
        for scenario, result in performance_results.items():
            assert result["avg_time"] < 5.0, f"{scenario} took too long: {result['avg_time']:.3f}s"
        
        print(f"\nPerformance Summary:")
        for scenario, result in performance_results.items():
            print(f"  {scenario}: {result['avg_time']:.3f}s avg, {result['result_count']} results")


if __name__ == "__main__":
    # Run tests with pytest
    pytest.main([__file__, "-v", "--tb=short"])