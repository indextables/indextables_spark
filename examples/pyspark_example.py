#!/usr/bin/env python3
"""
Tantivy4Spark Python Example

This script demonstrates how to use the Tantivy4Spark data source with PySpark
to write data, read it back, and perform various queries including full-text search.

Prerequisites:
- PySpark installed: pip install pyspark
- Tantivy4Spark JAR file available
- Python 3.7+

Usage:
    python example_tantivy4spark.py
"""

import tempfile
import shutil
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BooleanType, TimestampType
from pyspark.sql.functions import col, desc, asc, count, avg, sum as spark_sum
from datetime import datetime, timedelta
import os

# Configuration
TANTIVY4SPARK_JAR = "target/tantivy4spark-1.0.0-SNAPSHOT.jar"  # Adjust path as needed
TANTIVY4SPARK_JAR = "target/tantivy4spark-1.0.0-SNAPSHOT-shaded.jar"

def create_spark_session():
    """Create SparkSession with Tantivy4Spark configuration"""
    
    # Check if JAR exists
    jar_path = Path(TANTIVY4SPARK_JAR)
    if not jar_path.exists():
        print(f"Warning: JAR file not found at {TANTIVY4SPARK_JAR}")
        print("Please build the project first with: mvn clean package")
        print("Using SparkSession without JAR - some operations may fail")
        
    spark = SparkSession.builder \
        .appName("Tantivy4Spark Python Example") \
        .master("local[2]") \
        .config("spark.jars", str(jar_path) if jar_path.exists() else "") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created: {spark.version}")
    return spark

def create_sample_data(spark):
    """Create sample data with various data types for demonstration"""
    
    # Define schema
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("title", StringType(), False),
        StructField("content", StringType(), False),
        StructField("category", StringType(), False),
        StructField("author", StringType(), False),
        StructField("rating", DoubleType(), True),
        StructField("views", IntegerType(), True),
        StructField("published", BooleanType(), True),
        StructField("created_date", TimestampType(), True)
    ])
    
    # Sample data - articles/documents with full-text content
    sample_data = [
        (1, "Introduction to Apache Spark", 
         "Apache Spark is a unified analytics engine for large-scale data processing. It provides high-level APIs in Java, Scala, Python and R, and an optimized engine that supports general execution graphs.",
         "Technology", "John Doe", 4.5, 1500, True, datetime(2024, 1, 15, 10, 0, 0)),
        
        (2, "Machine Learning with Python", 
         "Python has become the go-to language for machine learning and data science. With libraries like scikit-learn, pandas, and numpy, Python provides powerful tools for data analysis.",
         "Technology", "Jane Smith", 4.8, 2300, True, datetime(2024, 2, 1, 14, 30, 0)),
        
        (3, "The Future of Artificial Intelligence", 
         "Artificial Intelligence is transforming industries across the globe. From healthcare to finance, AI technologies are creating new possibilities and solving complex problems.",
         "AI", "Bob Johnson", 4.2, 1800, True, datetime(2024, 1, 20, 9, 15, 0)),
        
        (4, "Database Design Principles", 
         "Good database design is crucial for application performance. This article covers normalization, indexing strategies, and query optimization techniques for modern databases.",
         "Database", "Alice Brown", 4.0, 1200, True, datetime(2024, 1, 10, 16, 45, 0)),
        
        (5, "Cloud Computing Best Practices", 
         "Cloud computing has revolutionized how we deploy and scale applications. Learn about microservices architecture, containerization, and serverless computing patterns.",
         "Cloud", "Charlie Wilson", 4.6, 2100, True, datetime(2024, 2, 5, 11, 20, 0)),
        
        (6, "Data Visualization Techniques", 
         "Effective data visualization helps communicate insights clearly. This guide explores different chart types, color theory, and interactive dashboard design principles.",
         "Analytics", "Diana Lee", 4.4, 1750, True, datetime(2024, 1, 25, 13, 10, 0)),
        
        (7, "Cybersecurity Fundamentals", 
         "Cybersecurity is more important than ever. Learn about threat modeling, encryption, authentication protocols, and security best practices for modern applications.",
         "Security", "Frank Miller", 4.7, 1950, True, datetime(2024, 2, 10, 8, 30, 0)),
        
        (8, "DevOps and Continuous Integration", 
         "DevOps practices improve software delivery speed and quality. Explore CI/CD pipelines, infrastructure as code, and monitoring strategies for reliable systems.",
         "DevOps", "Grace Davis", 4.3, 1650, False, datetime(2024, 2, 15, 12, 0, 0)),
        
        (9, "Blockchain Technology Overview", 
         "Blockchain technology enables decentralized applications and cryptocurrencies. Understand distributed ledgers, consensus mechanisms, and smart contract development.",
         "Blockchain", "Henry Chen", 3.9, 1400, True, datetime(2024, 1, 30, 15, 45, 0)),
        
        (10, "Internet of Things Applications", 
         "IoT devices are creating new data streams and automation possibilities. Learn about sensor networks, edge computing, and real-time data processing for IoT systems.",
         "IoT", "Ivy Taylor", 4.1, 1300, True, datetime(2024, 2, 3, 10, 15, 0))
    ]
    
    df = spark.createDataFrame(sample_data, schema)
    print(f"✅ Created sample dataset with {df.count()} records")
    return df

def write_to_tantivy(df, output_path):
    """Write DataFrame to Tantivy4Spark format"""
    
    print(f"📝 Writing data to Tantivy format at: {output_path}")
    
    try:
        df.write \
          .format("tantivy4spark") \
          .mode("overwrite") \
          .save(output_path)
        
        print("✅ Data written successfully to Tantivy format")
        
        # List the output files
        output_dir = Path(output_path)
        if output_dir.exists():
            files = list(output_dir.rglob("*"))
            print(f"📁 Output contains {len(files)} files/directories:")
            for f in sorted(files)[:10]:  # Show first 10 files
                print(f"   {f.relative_to(output_dir)}")
            if len(files) > 10:
                print(f"   ... and {len(files) - 10} more")
                
    except Exception as e:
        print(f"❌ Error writing to Tantivy format: {e}")
        print("This might be expected if the native library is not available")
        return False
    
    return True

def read_from_tantivy(spark, input_path):
    """Read DataFrame from Tantivy4Spark format"""
    
    print(f"📖 Reading data from Tantivy format at: {input_path}")
    
    try:
        df = spark.read \
               .format("tantivy4spark") \
               .load(input_path)
        
        count = df.count()
        print(f"✅ Read {count} records from Tantivy format")
        return df
        
    except Exception as e:
        print(f"❌ Error reading from Tantivy format: {e}")
        print("This might be expected if the native library is not available")
        return None

def run_basic_queries(df):
    """Run basic SQL queries on the dataset"""
    
    print("\n🔍 Running Basic Queries")
    print("=" * 50)
    
    # Create temporary view for SQL queries
    df.createOrReplaceTempView("articles")
    
    # Query 1: Count by category
    print("📊 Articles by category:")
    category_counts = df.groupBy("category").count().orderBy(desc("count"))
    category_counts.show()
    
    # Query 2: Top rated articles
    print("⭐ Top rated articles:")
    df.select("title", "author", "rating", "views") \
      .orderBy(desc("rating"), desc("views")) \
      .limit(5) \
      .show(truncate=False)
    
    # Query 3: Articles with high engagement (rating > 4.0 and views > 1500)
    print("🔥 High engagement articles:")
    high_engagement = df.filter((col("rating") > 4.0) & (col("views") > 1500))
    high_engagement.select("title", "category", "rating", "views").show(truncate=False)
    
    # Query 4: Average rating and total views by category
    print("📈 Category statistics:")
    df.groupBy("category") \
      .agg(
          count("*").alias("article_count"),
          avg("rating").alias("avg_rating"),
          spark_sum("views").alias("total_views")
      ) \
      .orderBy(desc("avg_rating")) \
      .show()

def run_text_search_queries(spark):
    """Run text search queries using SQL (these would use Tantivy's full-text search)"""
    
    print("\n🔍 Running Text Search Queries")
    print("=" * 50)
    
    # Note: These queries demonstrate the syntax but may not work without the native library
    print("Note: Full-text search requires the Tantivy native library to be loaded")
    
    try:
        # Search for articles containing "machine learning"
        print("🔎 Searching for 'machine learning':")
        search_results = spark.sql("""
            SELECT title, author, rating, content
            FROM articles 
            WHERE content LIKE '%machine learning%' OR title LIKE '%Machine Learning%'
            ORDER BY rating DESC
        """)
        search_results.show(truncate=False)
        
        # Search for articles about "data"
        print("🔎 Searching for articles about 'data':")
        data_articles = spark.sql("""
            SELECT title, category, author, views
            FROM articles 
            WHERE content LIKE '%data%' OR title LIKE '%Data%'
            ORDER BY views DESC
        """)
        data_articles.show(truncate=False)
        
        # Search by author
        print("🔎 Articles by specific authors:")
        author_search = spark.sql("""
            SELECT author, COUNT(*) as article_count, AVG(rating) as avg_rating
            FROM articles 
            WHERE author LIKE '%John%' OR author LIKE '%Jane%'
            GROUP BY author
        """)
        author_search.show()
        
    except Exception as e:
        print(f"❌ Error running text search queries: {e}")

def run_advanced_queries(spark, df):
    """Run more advanced analytical queries"""
    
    print("\n📊 Running Advanced Analytics")
    print("=" * 50)
    
    # Create temporary view
    df.createOrReplaceTempView("articles")
    
    # Query 1: Articles published in the last 30 days
    print("📅 Recent articles (last 30 days):")
    recent_articles = df.filter(
        col("created_date") >= (datetime.now() - timedelta(days=30))
    )
    recent_articles.select("title", "category", "created_date", "published") \
                   .orderBy(desc("created_date")) \
                   .show(truncate=False)
    
    # Query 2: Performance metrics by month
    print("📈 Monthly performance metrics:")
    monthly_stats = spark.sql("""
        SELECT 
            MONTH(created_date) as month,
            YEAR(created_date) as year,
            COUNT(*) as articles_published,
            AVG(rating) as avg_rating,
            AVG(views) as avg_views,
            SUM(CASE WHEN published = true THEN 1 ELSE 0 END) as published_count
        FROM articles
        GROUP BY YEAR(created_date), MONTH(created_date)
        ORDER BY year, month
    """)
    monthly_stats.show()
    
    # Query 3: Top authors by engagement
    print("👥 Top authors by engagement:")
    author_engagement = spark.sql("""
        SELECT 
            author,
            COUNT(*) as total_articles,
            AVG(rating) as avg_rating,
            SUM(views) as total_views,
            SUM(views) / COUNT(*) as avg_views_per_article
        FROM articles
        GROUP BY author
        HAVING COUNT(*) >= 1
        ORDER BY avg_rating DESC, total_views DESC
    """)
    author_engagement.show()

def demonstrate_tantivy_features():
    """Main function demonstrating Tantivy4Spark features"""
    
    print("🚀 Tantivy4Spark Python Example")
    print("=" * 50)
    
    # Create temporary directory for output
    temp_dir = tempfile.mkdtemp(prefix="tantivy4spark_")
    output_path = os.path.join(temp_dir, "articles_tantivy")
    
    spark = None
    
    try:
        # Initialize Spark
        spark = create_spark_session()
        
        # Create sample data
        df = create_sample_data(spark)
        
        # Show sample of the data
        print("\n📋 Sample of the dataset:")
        df.select("id", "title", "category", "author", "rating", "views").show()
        
        # Write to Tantivy format
        write_success = write_to_tantivy(df, output_path)
        
        if write_success:
            # Read back from Tantivy format
            read_df = read_from_tantivy(spark, output_path)
            
            if read_df is not None:
                # Run queries on the data read from Tantivy
                run_basic_queries(read_df)
                run_text_search_queries(spark)
                run_advanced_queries(spark, read_df)
            else:
                print("⚠️  Unable to read from Tantivy format, running queries on original DataFrame")
                run_basic_queries(df)
                run_text_search_queries(spark)
                run_advanced_queries(df)
        else:
            print("⚠️  Unable to write to Tantivy format, running queries on original DataFrame")
            run_basic_queries(df)
            run_text_search_queries(spark)
            run_advanced_queries(df)
        
        print(f"\n✅ Example completed successfully!")
        print(f"📁 Temporary files created in: {temp_dir}")
        
    except Exception as e:
        print(f"❌ Error in main execution: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # Cleanup
        if spark:
            spark.stop()
            print("🛑 Spark session stopped")
        
        # Optionally clean up temp directory
        try:
            shutil.rmtree(temp_dir)
            print(f"🧹 Cleaned up temporary directory: {temp_dir}")
        except Exception as e:
            print(f"⚠️  Could not clean up temp directory: {e}")

def print_usage_instructions():
    """Print usage instructions"""
    
    print("\n" + "=" * 60)
    print("📚 Usage Instructions")
    print("=" * 60)
    print("""
To run this example:

1. Build the Tantivy4Spark JAR:
   mvn clean package

2. Install PySpark:
   pip install pyspark

3. Run the script:
   python example_tantivy4spark.py

Expected behavior:
- If the JAR is available and native library loads: Full functionality
- If the JAR is missing or native library fails: Fallback to regular DataFrame operations

The script demonstrates:
✓ SparkSession configuration with Tantivy4Spark
✓ Creating sample data with various data types
✓ Writing to Tantivy4Spark format
✓ Reading from Tantivy4Spark format  
✓ Basic SQL queries and aggregations
✓ Text search capabilities (when native library is available)
✓ Advanced analytics with date/time functions
✓ Error handling and graceful degradation
""")

if __name__ == "__main__":
    demonstrate_tantivy_features()
    print_usage_instructions()
