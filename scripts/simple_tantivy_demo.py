#!/usr/bin/env python3
"""
Simple Tantivy4Spark demo script.

Creates 1 million records and saves them using Tantivy4Spark format.
"""

import os
import time
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


def main():
    # Find the JAR file (adjust path as needed)
    jar_path = "../target/tantivy4spark-1.0.0-SNAPSHOT-shaded.jar"
    if not os.path.exists(jar_path):
        jar_path = "target/tantivy4spark-1.0.0-SNAPSHOT.jar"
    
    if not os.path.exists(jar_path):
        print("JAR not found. Run 'mvn clean package' first.")
        return
    
    # Initialize Spark with Tantivy4Spark
    spark = (SparkSession.builder
             .appName("Tantivy4Spark-Demo")
             .config("spark.jars", jar_path)
             .config("spark.driver.memory", "4g")
             .getOrCreate())
    
    print("Creating 1 million records...")
    
    # Generate 1M records
    data = []
    for i in range(1000000):
        data.append((
            i,                                      # id
            f"User_{i}",                           # name
            f"user{i}@example.com",                # email
            f"Content for record {i} with searchable text about technology and data science",  # content
            25 + (i % 40),                         # age
            50000 + (i % 100000),                  # salary
            i % 2 == 0                             # active
        ))
    
    # Create DataFrame
    schema = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("content", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("salary", IntegerType(), True),
        StructField("active", BooleanType(), True)
    ])
    
    df = spark.createDataFrame(data, schema)
    
    # Save using Tantivy4Spark format
    output_path = "file:///tmp/tantivy_demo"
    print(f"Saving to {output_path}...")
    
    start_time = time.time()
    df.write.format("tantivy4spark").mode("overwrite").save(output_path)
    write_time = time.time() - start_time
    
    print(f"✓ Saved 1M records in {write_time:.1f}s")
    
    # Read back and test
    read_df = spark.read.format("tantivy4spark").load(output_path)
    count = read_df.count()
    
    print(f"✓ Read back {count:,} records")
    print("✓ Demo completed!")
    
    spark.stop()


if __name__ == "__main__":
    main()
