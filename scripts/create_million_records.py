#!/usr/bin/env python3
"""
Simple script to create 1 million records and save using Tantivy4Spark format.

This script demonstrates:
- Spark session initialization with Tantivy4Spark
- Large dataset generation (1M records)
- Writing data in Tantivy4Spark format
- Basic performance measurement
"""

import os
import sys
import time
from datetime import datetime, date, timedelta
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import avg


def find_tantivy_jar():
    """Find the Tantivy4Spark JAR file."""
    # Look for JAR in common locations
    possible_paths = [
        "../target/tantivy4spark-1.0.0-SNAPSHOT-shaded.jar",
    ]
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    for path in possible_paths:
        full_path = os.path.join(script_dir, path)
        if os.path.exists(full_path):
            return os.path.abspath(full_path)
    
    # If not found, try to build
    print("JAR not found. Please run 'mvn clean package' first.")
    return None


def create_spark_session():
    """Initialize Spark session with Tantivy4Spark."""
    jar_path = find_tantivy_jar()
    if not jar_path:
        sys.exit(1)
    
    print(f"Using JAR: {jar_path}")
    
    spark = (SparkSession.builder
             .appName("Tantivy4Spark-Million-Records-Demo")
             .config("spark.jars", jar_path)
             .config("spark.driver.memory", "8g")
             .config("spark.executor.memory", "8g")
             .config("spark.driver.maxResultSize", "2g")
             #.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             #.config("spark.sql.adaptive.enabled", "true")
             #.config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .getOrCreate())
    
    spark.sparkContext.setLogLevel("WARN")
    return spark


def generate_million_records(spark, num_records=1000000):
    """Generate a DataFrame with the specified number of records."""
    print(f"Generating {num_records:,} records...")
    
    # Sample data for realistic content
    companies = [
        "TechCorp", "DataSoft", "CloudTech", "InnovateLabs", "FutureSystems",
        "SmartSolutions", "DigitalWorks", "ByteForce", "CodeCraft", "NetVision"
    ]
    
    departments = [
        "Engineering", "Data Science", "Product", "Marketing", "Sales", 
        "Operations", "HR", "Finance", "Customer Success", "Research"
    ]
    
    cities = [
        "San Francisco", "New York", "Seattle", "Austin", "Denver",
        "Boston", "Los Angeles", "Chicago", "Atlanta", "Portland"
    ]
    
    job_titles = [
        "Software Engineer", "Data Scientist", "Product Manager", "Designer",
        "DevOps Engineer", "Research Scientist", "Sales Engineer", "Analyst",
        "Technical Lead", "Principal Engineer", "Staff Engineer", "Manager"
    ]
    
    # Generate data in batches to avoid memory issues
    batch_size = 100000
    all_data = []
    
    start_time = time.time()
    
    for batch in range(0, num_records, batch_size):
        batch_end = min(batch + batch_size, num_records)
        batch_data = []
        
        for i in range(batch, batch_end):
            # Generate realistic employee record
            company = companies[i % len(companies)]
            department = departments[i % len(departments)]
            city = cities[i % len(cities)]
            title = job_titles[i % len(job_titles)]
            
            # Generate realistic values with some variation
            base_salary = 60000 + (hash(f"{company}_{department}_{title}") % 100000)
            age = 22 + (i % 43)  # Ages 22-64
            experience = min(age - 22, 25)  # Max 25 years experience
            
            # Create start date (last 10 years)
            start_date = date(2014, 1, 1) + timedelta(days=(i % 3653))
            
            # Create realistic email and profile
            email = f"employee{i}@{company.lower()}.com"
            profile = f"{title} at {company} in {department}. Based in {city} with {experience} years of experience."
            
            batch_data.append((
                i,                                    # employee_id 
                f"Employee_{i}",                     # name
                email,                               # email
                company,                             # company
                department,                          # department
                title,                               # job_title
                city,                                # location
                age,                                 # age
                experience,                          # years_experience
                base_salary + (i % 50000),          # salary (with variation)
                round(1.0 + (i % 10) * 0.5, 1),    # performance_rating (1.0-5.5)
                i % 2 == 0,                          # is_active
                start_date,                          # start_date
                datetime.now(),                      # last_updated
                profile                              # profile_text
            ))
        
        all_data.extend(batch_data)
        
        if batch % (batch_size * 5) == 0:  # Progress update every 500k records
            elapsed = time.time() - start_time
            print(f"  Generated {batch_end:,} records in {elapsed:.1f}s")
    
    # Define schema
    schema = StructType([
        StructField("employee_id", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("email", StringType(), True),
        StructField("company", StringType(), True),
        StructField("department", StringType(), True),
        StructField("job_title", StringType(), True),
        StructField("location", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("years_experience", IntegerType(), True),
        StructField("salary", IntegerType(), True),
        StructField("performance_rating", DoubleType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("start_date", DateType(), True),
        StructField("last_updated", TimestampType(), True),
        StructField("profile_text", StringType(), True)
    ])
    
    # Create DataFrame
    df = spark.createDataFrame(all_data, schema)
    
    generation_time = time.time() - start_time
    print(f"✓ Generated {num_records:,} records in {generation_time:.1f}s")
    
    return df


def save_as_tantivy(df, output_path):
    """Save DataFrame using Tantivy4Spark format."""
    print(f"Saving to Tantivy4Spark format: {output_path}")
    
    start_time = time.time()
    
    # Save with partitioning for better query performance
    (df.write
       .format("tantivy4spark")
       .mode("overwrite")
       .partitionBy("company", "department")  # Partition by company and department
       .save(output_path))
    
    write_time = time.time() - start_time
    print(f"✓ Data saved in {write_time:.1f}s")
    
    return write_time


def read_data(spark, input_path):
    print("Reading data")

    df = spark.read.format('tantivy4spark').load(input_path)
    df.createOrReplaceTempView("tempview")
    res = spark.sql("select * from tempview where email='employee6999@techcorp.com' limit 10")
    print(f"Count is={res.count()}")
    print(f"Result is={res.collect()}")


def verify_data(spark, input_path):
    """Read back and verify the saved data."""
    print(f"Verifying saved data...")
    
    start_time = time.time()
    
    # Read the data back
    df = spark.read.format("tantivy4spark").load(input_path)
    
    # Basic verification
    count = df.count()
    read_time = time.time() - start_time
    
    print(f"✓ Read {count:,} records in {read_time:.1f}s")
    
    # Show sample data
    print("\nSample records:")
    df.select("employee_id", "name", "company", "department", "job_title", "salary").show(5)
    
    # Test some queries
    print("\nRunning sample queries...")
    
    # Query 1: Count by company
    company_counts = df.groupBy("company").count().orderBy("count", ascending=False)
    print("\nTop companies by employee count:")
    company_counts.show(5)
    
    # Query 2: Average salary by department
    dept_salaries = df.groupBy("department").agg(
        avg("salary").alias("avg_salary"),
        count("*").alias("employee_count")
    ).orderBy("avg_salary", ascending=False)
    print("\nAverage salary by department:")
    dept_salaries.show(5)
    
    # Query 3: Text search on profile
    text_search_start = time.time()
    engineers = df.filter(col("profile_text").contains("Engineer")).count()
    text_search_time = time.time() - text_search_start
    print(f"\nText search results: {engineers:,} profiles contain 'Engineer' (found in {text_search_time:.3f}s)")
    
    # Query 4: Complex query
    complex_start = time.time()
    senior_engineers = df.filter(
        (col("job_title").contains("Engineer")) &
        (col("years_experience") >= 5) &
        (col("salary") > 80000) &
        (col("is_active") == True)
    ).count()
    complex_time = time.time() - complex_start
    print(f"Complex query: {senior_engineers:,} senior engineers found in {complex_time:.3f}s")
    
    return read_time


def main():
    """Main execution function."""
    print("Tantivy4Spark Million Records Demo")
    print("=" * 50)
    
    # Configuration
    num_records = 100000
    output_path = "file:/tmp/tantivy4spark_million_records/"
    
    print(f"Creating {num_records:,} records and saving to: {output_path}")
    print()
    
    # Initialize Spark
    spark = create_spark_session()
    
    try:
        # Generate data
        df = generate_million_records(spark, num_records)
        
        # Save using Tantivy4Spark
        write_time = save_as_tantivy(df, output_path)
        
        # Verify the saved data
        #read_time = verify_data(spark, output_path)
        read_time = read_data(spark, output_path)    
        # Summary
        print("\n" + "=" * 50)
        print("PERFORMANCE SUMMARY")
        print("=" * 50)
        print(f"Records created: {num_records:,}")
        print(f"Write time: {write_time:.1f}s ({num_records/write_time:,.0f} records/sec)")
        #print(f"Read time: {read_time:.1f}s ({num_records/read_time:,.0f} records/sec)")
        print(f"Storage location: {output_path}")
        print("\n✓ Demo completed successfully!")
        
    except Exception as e:
        print(f"✗ Error: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
