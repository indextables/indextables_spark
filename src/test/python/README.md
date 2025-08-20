# Tantivy4Spark PySpark Integration Tests

This directory contains comprehensive PySpark integration tests for Tantivy4Spark, validating all functionality from Python applications.

## Test Files

### `test_tantivy4spark_integration.py`
**Comprehensive integration test suite covering:**
- ✅ Basic write/read operations
- ✅ All supported data types (String, Integer, Long, Float, Double, Boolean, Binary, Timestamp, Date)
- ✅ Unsupported data type rejection (Arrays, Maps, Structs)
- ✅ Text search operations (contains, startsWith, endsWith)
- ✅ Numeric range queries (>, <, between, ==, IN)
- ✅ Boolean and null value handling
- ✅ Aggregation operations (count, avg, max, min, groupBy)
- ✅ Complex compound queries (AND/OR combinations)
- ✅ Partitioned data operations
- ✅ Schema evolution scenarios
- ✅ Error handling and edge cases
- ✅ Performance testing with large datasets (1000+ records)
- ✅ Append mode operations

### `test_bloom_filter_acceleration.py`
**Specialized bloom filter acceleration tests:**
- ✅ Text search acceleration validation
- ✅ Multiple term searches (AND/OR combinations)
- ✅ Prefix and suffix search patterns
- ✅ Case sensitivity handling
- ✅ Non-existent term optimization
- ✅ Combined filters (text + numeric + boolean)
- ✅ Performance benchmarking

## Prerequisites

### Java Environment
```bash
# Java 11+ required
java -version
```

### Project Build
```bash
# Build the shaded JAR first
cd /path/to/tantivy4spark
mvn clean package
```

### Python Dependencies
```bash
# Install test dependencies
pip install -r requirements.txt
```

## Running Tests

### Option 1: Automated Test Runner (Recommended)
```bash
# Run all tests with environment setup
python run_tests.py

# Run with options
python run_tests.py --parallel --coverage --html-report

# Run specific test pattern
python run_tests.py --test-filter "test_basic_write_read"

# Skip dependency installation
python run_tests.py --skip-deps
```

### Option 2: Direct Pytest Execution
```bash
# Run all integration tests
pytest test_tantivy4spark_integration.py -v

# Run bloom filter tests only
pytest test_bloom_filter_acceleration.py -v

# Run specific test
pytest test_tantivy4spark_integration.py::TestTantivy4SparkIntegration::test_text_search_queries -v

# Run with coverage
pytest --cov=. --cov-report=html test_tantivy4spark_integration.py
```

### Option 3: Individual Test Methods
```python
# Run specific test categories
pytest -k "text_search" -v          # Text search tests
pytest -k "numeric" -v              # Numeric operation tests  
pytest -k "aggregation" -v          # Aggregation tests
pytest -k "schema" -v               # Schema handling tests
pytest -k "performance" -v          # Performance tests
pytest -k "bloom_filter" -v         # Bloom filter tests
```

## Test Configuration

### Environment Variables
```bash
# Optional: Specify JAR location
export TANTIVY4SPARK_JAR="/path/to/tantivy4spark-1.0.0-SNAPSHOT-shaded.jar"

# Optional: Spark configuration
export SPARK_HOME="/path/to/spark"
export PYTHONPATH="$SPARK_HOME/python:$PYTHONPATH"
```

### Spark Configuration
The tests automatically configure Spark with:
- **JAR Loading**: Automatic shaded JAR detection and loading
- **Memory**: 2GB driver and executor memory
- **Adaptive Query Execution**: Enabled for performance
- **Logging**: WARN level to reduce noise
- **Serializer**: KryoSerializer for efficiency

## Test Data

### Realistic Test Datasets
Tests use realistic data patterns:
- **Employee Records**: Names, departments, salaries, demographics
- **Product Catalogs**: Names, descriptions, categories, pricing
- **Research Papers**: Titles, abstracts, content, authors, metadata
- **Event Logs**: Timestamps, categories, values, partitioned data

### Data Volumes
- **Basic Tests**: 10-100 records for functionality validation
- **Performance Tests**: 1000-2000 records for load testing
- **Bloom Filter Tests**: 1000-2000 records across multiple partitions

### Data Types Coverage
```python
# All supported Spark → Tantivy type mappings tested:
IntegerType    → i64
LongType       → i64  
StringType     → text
FloatType      → f64
DoubleType     → f64
BooleanType    → i64 (0/1)
BinaryType     → bytes
TimestampType  → i64 (epoch millis)
DateType       → i64 (days since epoch)

# Unsupported types (properly rejected):
ArrayType      → UnsupportedOperationException
MapType        → UnsupportedOperationException  
StructType     → UnsupportedOperationException
```

## Expected Output

### Successful Test Run
```
======================== test session starts ========================
collecting ... collected 25 items

test_tantivy4spark_integration.py::TestTantivy4SparkIntegration::test_basic_write_read_cycle PASSED [ 4%]
test_tantivy4spark_integration.py::TestTantivy4SparkIntegration::test_comprehensive_data_types PASSED [ 8%]
test_tantivy4spark_integration.py::TestTantivy4SparkIntegration::test_unsupported_data_types PASSED [12%]
...
test_bloom_filter_acceleration.py::TestBloomFilterAcceleration::test_bloom_filter_text_search_acceleration PASSED [88%]
test_bloom_filter_acceleration.py::TestBloomFilterAcceleration::test_bloom_filter_performance_comparison PASSED [100%]

======================== 25 passed in 45.67s ========================
```

### Performance Metrics
```
Write time for 1000 records: 2.34 seconds
Read time for 1000 records: 0.89 seconds
Search time: 0.12 seconds, Results: 156

Bloom Filter Performance Summary:
  Common term: 0.145s avg, 234 results
  Specific term: 0.098s avg, 67 results  
  Rare term: 0.076s avg, 12 results
  Non-existent: 0.023s avg, 0 results
```

## Troubleshooting

### Common Issues

**JAR Not Found**
```bash
# Build the project first
mvn clean package

# Verify JAR exists
ls target/*.jar
```

**Python Dependencies**
```bash
# Install PySpark and test dependencies
pip install pyspark>=3.5.0 pytest>=7.0.0
```

**Memory Issues**
```bash
# Increase Spark memory if needed
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

**Permission Issues**
```bash
# Ensure write permissions for temp directories
chmod 755 /tmp
```

### Debugging Failed Tests
```bash
# Run with verbose output and no capture
pytest test_tantivy4spark_integration.py -v -s --tb=long

# Run single test with maximum detail
pytest test_tantivy4spark_integration.py::TestTantivy4SparkIntegration::test_basic_write_read_cycle -v -s --tb=long --capture=no
```

## Integration with CI/CD

### GitHub Actions Example
```yaml
name: PySpark Integration Tests
on: [push, pull_request]
jobs:
  test:
    runs-on: ubuntu-latest
    steps:
    - uses: actions/checkout@v3
    - uses: actions/setup-java@v3
      with:
        java-version: '11'
    - uses: actions/setup-python@v4
      with:
        python-version: '3.9'
    - name: Build JAR
      run: mvn clean package
    - name: Install Python dependencies
      run: pip install -r src/test/python/requirements.txt
    - name: Run PySpark integration tests
      run: python src/test/python/run_tests.py --skip-deps
```

This comprehensive test suite ensures that Tantivy4Spark works correctly from PySpark applications with full feature coverage and performance validation.