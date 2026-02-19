# JSON Field Integration Design for IndexTables4Spark

**Version**: 1.0
**Status**: Design Phase
**Target Release**: v2.1
**Dependencies**: tantivy4java 0.25.2+

---

## Executive Summary

This design extends IndexTables4Spark to leverage tantivy4java's JSON field capabilities, enabling support for complex nested data types (Struct, Array) and flexible JSON string indexing. This eliminates the current limitation where complex types throw `UnsupportedOperationException` and provides powerful query capabilities for semi-structured data.

**Key Capabilities:**
1. **Struct Type Support**: Spark StructType columns stored as JSON objects with full predicate pushdown using dot notation
2. **Array Type Support**: Spark ArrayType columns stored as JSON arrays with predicate pushdown including `array_contains` operations
3. **JSON String Indexing**: String columns optionally parsed and stored as JSON for IndexQuery operations with JSONPath syntax

**Business Value:**
- ✅ Support modern data formats (nested Parquet, Avro, JSON)
- ✅ Enable complex analytical queries on semi-structured data
- ✅ Maintain high performance with native predicate pushdown
- ✅ Preserve existing functionality with backward compatibility

---

## Table of Contents

1. [Feature 1: Struct Type Support](#feature-1-struct-type-support)
2. [Feature 2: Array Type Support](#feature-2-array-type-support)
3. [Feature 3: JSON String Indexing](#feature-3-json-string-indexing)
4. [Architecture & Implementation](#architecture--implementation)
5. [Configuration & API Design](#configuration--api-design)
6. [Predicate Pushdown Strategy](#predicate-pushdown-strategy)
7. [Test Plan](#test-plan)
8. [Migration & Backward Compatibility](#migration--backward-compatibility)
9. [Performance Considerations](#performance-considerations)
10. [Implementation Roadmap](#implementation-roadmap)

---

## Feature 1: Struct Type Support

### Overview

Spark StructType columns will be seamlessly converted to JSON objects in tantivy4java, enabling nested field queries with full predicate pushdown support.

### Current Behavior

```scala
case class User(name: String, age: Int, address: Address)
case class Address(city: String, zip: String, coordinates: Coordinates)
case class Coordinates(lat: Double, lon: Double)

val df = spark.createDataFrame(users).toDF()

// Current behavior: FAILS with UnsupportedOperationException
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")
// Error: Arrays, Maps, Structs not supported
```

### New Behavior

```scala
// NEW: Struct types automatically supported
val schema = StructType(Seq(
  StructField("id", StringType),
  StructField("user", StructType(Seq(
    StructField("name", StringType),
    StructField("age", IntegerType),
    StructField("address", StructType(Seq(
      StructField("city", StringType),
      StructField("zip", StringType),
      StructField("coordinates", StructType(Seq(
        StructField("lat", DoubleType),
        StructField("lon", DoubleType)
      )))
    )))
  )))
))

// Write works seamlessly
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")

// Read with nested queries
val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")

// Predicate pushdown on nested fields
result.filter($"user.age" > 25).show()
result.filter($"user.address.city" === "New York").show()
result.filter($"user.address.coordinates.lat" > 40.0).show()

// Complex nested predicates
result.filter(
  $"user.age" > 25 &&
  $"user.address.city" === "New York" &&
  $"user.address.coordinates.lat" > 40.0
).show()
```

### Technical Implementation

#### Schema Mapping

**Spark Schema → tantivy4java Schema:**
```scala
// Spark StructType
StructField("user", StructType(Seq(
  StructField("name", StringType),
  StructField("age", IntegerType)
)))

// Maps to tantivy4java
Field userField = builder.addJsonField("user", JsonObjectOptions.full());
```

#### Data Marshalling (Write Path)

```scala
// Spark Row → JSON Map
def sparkStructToJsonMap(row: Row, structType: StructType): java.util.Map[String, Object] = {
  val map = new java.util.HashMap[String, Object]()

  structType.fields.zipWithIndex.foreach { case (field, idx) =>
    val value = row.get(idx)
    if (value != null) {
      val jsonValue = field.dataType match {
        case st: StructType =>
          sparkStructToJsonMap(value.asInstanceOf[Row], st)
        case at: ArrayType =>
          sparkArrayToJsonList(value, at)
        case _ =>
          primitiveToJavaObject(value, field.dataType)
      }
      map.put(field.name, jsonValue)
    }
  }
  map
}

// Example: Add to tantivy4java document
doc.addJson(userField, sparkStructToJsonMap(row.getAs[Row]("user"), userStructType))
```

#### Data Unmarshalling (Read Path)

```scala
// JSON Map → Spark Row
def jsonMapToSparkRow(
  jsonMap: java.util.Map[String, Object],
  structType: StructType
): Row = {
  val values = structType.fields.map { field =>
    val jsonValue = jsonMap.get(field.name)
    if (jsonValue == null) {
      null
    } else {
      field.dataType match {
        case st: StructType =>
          jsonMapToSparkRow(jsonValue.asInstanceOf[java.util.Map[String, Object]], st)
        case at: ArrayType =>
          jsonListToSparkArray(jsonValue.asInstanceOf[java.util.List[_]], at)
        case _ =>
          javaObjectToPrimitive(jsonValue, field.dataType)
      }
    }
  }
  Row.fromSeq(values)
}
```

#### Predicate Pushdown

**Catalyst Filter → tantivy4java JSON Query:**
```scala
// Spark filter: $"user.address.city" === "New York"
// Translates to:
Query query = Query.jsonTermQuery(schema, "user", "address.city", "New York");

// Spark filter: $"user.age" > 25
// Translates to:
Query query = Query.jsonRangeQuery(
  schema, "user", "age",
  26L,    // lower bound (exclusive translated to inclusive)
  null,   // no upper bound
  true, true
);

// Spark filter: $"user.address.city" === "New York" && $"user.age" > 25
// Translates to:
Query q1 = Query.jsonTermQuery(schema, "user", "address.city", "New York");
Query q2 = Query.jsonRangeQuery(schema, "user", "age", 26L, null, true, true);
Query combined = Query.booleanQuery(
  Arrays.asList(q1, q2),
  Arrays.asList(Occur.MUST, Occur.MUST)
);
```

### Supported Predicate Types

| Spark Filter | tantivy4java Query | Pushdown Support |
|--------------|-------------------|------------------|
| `$"struct.field" === value` | `jsonTermQuery` | ✅ Full |
| `$"struct.field" > value` | `jsonRangeQuery` | ✅ Full |
| `$"struct.field" < value` | `jsonRangeQuery` | ✅ Full |
| `$"struct.field" >= value` | `jsonRangeQuery` | ✅ Full |
| `$"struct.field" <= value` | `jsonRangeQuery` | ✅ Full |
| `$"struct.field".isNull` | `NOT jsonExistsQuery` | ✅ Full |
| `$"struct.field".isNotNull` | `jsonExistsQuery` | ✅ Full |
| Complex AND/OR/NOT | `booleanQuery` | ✅ Full |
| `$"struct.field".contains(str)` | Not supported | ❌ Spark fallback |
| `$"struct.field".startsWith(str)` | Not supported | ❌ Spark fallback |

---

## Feature 2: Array Type Support

### Overview

Spark ArrayType columns will be stored as JSON arrays in tantivy4java, with support for array membership queries and element-level predicates.

### Current Behavior

```scala
case class Product(id: String, tags: Seq[String], prices: Seq[Double])

val df = spark.createDataFrame(products).toDF()

// Current behavior: FAILS
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")
// Error: Arrays not supported
```

### New Behavior

```scala
// NEW: Array types automatically supported
val schema = StructType(Seq(
  StructField("id", StringType),
  StructField("tags", ArrayType(StringType)),
  StructField("prices", ArrayType(DoubleType)),
  StructField("reviews", ArrayType(StructType(Seq(
    StructField("rating", IntegerType),
    StructField("comment", StringType)
  ))))
))

// Write works seamlessly
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")

// Read with array queries
val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")

// Array membership queries (pushdown supported)
result.filter(array_contains($"tags", "electronics")).show()
result.filter(array_contains($"tags", "laptop")).show()

// Array element range queries (pushdown supported)
result.filter($"prices".getItem(0) < 100.0).show()  // First price < 100

// Nested array queries (struct arrays)
result.filter($"reviews.rating" > 4).show()  // Any review rating > 4
```

### Technical Implementation

#### Data Marshalling (Write Path)

```scala
// Spark Array → JSON List
def sparkArrayToJsonList(value: Any, arrayType: ArrayType): java.util.List[Object] = {
  val sparkSeq = value.asInstanceOf[Seq[_]]
  val javaList = new java.util.ArrayList[Object]()

  arrayType.elementType match {
    case st: StructType =>
      sparkSeq.foreach { element =>
        javaList.add(sparkStructToJsonMap(element.asInstanceOf[Row], st))
      }
    case at: ArrayType =>
      sparkSeq.foreach { element =>
        javaList.add(sparkArrayToJsonList(element, at))
      }
    case _ =>
      sparkSeq.foreach { element =>
        if (element != null) {
          javaList.add(primitiveToJavaObject(element, arrayType.elementType))
        }
      }
  }
  javaList
}

// Example: Add to tantivy4java document
doc.addJson(tagsField, sparkArrayToJsonList(row.getAs[Seq[String]]("tags"), ArrayType(StringType)))
```

#### Data Unmarshalling (Read Path)

```scala
// JSON List → Spark Array
def jsonListToSparkArray(
  jsonList: java.util.List[_],
  arrayType: ArrayType
): Seq[_] = {
  import scala.jdk.CollectionConverters._

  arrayType.elementType match {
    case st: StructType =>
      jsonList.asScala.map { element =>
        jsonMapToSparkRow(element.asInstanceOf[java.util.Map[String, Object]], st)
      }.toSeq
    case at: ArrayType =>
      jsonList.asScala.map { element =>
        jsonListToSparkArray(element.asInstanceOf[java.util.List[_]], at)
      }.toSeq
    case _ =>
      jsonList.asScala.map { element =>
        javaObjectToPrimitive(element, arrayType.elementType)
      }.toSeq
  }
}
```

#### Predicate Pushdown for Arrays

**Array Contains:**
```scala
// Spark filter: array_contains($"tags", "laptop")
// Translates to:
Query query = Query.jsonTermQuery(schema, "tags", "", "laptop");
// Note: Empty path searches array elements directly
```

**Array Element Access:**
```scala
// Spark filter: $"prices".getItem(0) < 100
// Challenge: tantivy4java doesn't support indexed array access
// Solution: Push down as "any element matches" query
Query query = Query.jsonRangeQuery(schema, "prices", "", null, 100L, true, false);
// Matches if ANY price < 100 (conservative but correct)
```

**Nested Array Queries:**
```scala
// Spark filter: $"reviews.rating" > 4
// Translates to:
Query query = Query.jsonRangeQuery(schema, "reviews", "rating", 5L, null, true, true);
// Matches if ANY review has rating > 4
```

### Supported Array Predicate Types

| Spark Filter | tantivy4java Query | Pushdown Support |
|--------------|-------------------|------------------|
| `array_contains($"arr", value)` | `jsonTermQuery` | ✅ Full |
| `$"arr.field" === value` | `jsonTermQuery` (nested) | ✅ Full |
| `$"arr.field" > value` | `jsonRangeQuery` (nested) | ✅ Full |
| `size($"arr") === n` | Not supported | ❌ Spark fallback |
| `$"arr".getItem(0) === value` | `jsonTermQuery` (conservative) | ⚠️ Partial (any element) |
| `array_intersect($"arr", values)` | `booleanQuery` (multiple terms) | ✅ Full |

**Note on Array Index Access:**
- tantivy4java JSON queries match **any element** in an array
- Specific index access (`arr[0]`) is not supported natively
- For indexed access, we push down "any element matches" and let Spark post-filter
- This is conservative but correct (no false negatives)

---

## Feature 3: JSON String Indexing

### Overview

String columns can be optionally configured to parse JSON content and store as JSON objects in tantivy4java, enabling IndexQuery operations with JSONPath syntax.

### Use Case

```scala
// Event log table with JSON strings
case class Event(id: String, payload: String)
// payload contains: {"user_id": "123", "action": "click", "metadata": {"page": "/home"}}

val df = spark.createDataFrame(events).toDF()

// Configure payload field as JSON-indexed string
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexing.typemap.payload", "json")  // NEW: json type
  .save("s3://bucket/path")

// Query with JSONPath in IndexQuery
import org.apache.spark.sql.indextables.IndexQueryExpression._
val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/path")

// IndexQuery with JSONPath syntax
result.filter($"payload" indexquery "user_id:123").show()
result.filter($"payload" indexquery "action:click").show()
result.filter($"payload" indexquery "metadata.page:/home").show()

// Boolean combinations
result.filter($"payload" indexquery "user_id:123 AND action:click").show()
```

### Technical Implementation

#### Configuration

**New Field Type: `json`**
```scala
// Existing types: "string", "text"
// New type: "json" (for string columns containing JSON)

// Configuration options:
.option("spark.indextables.indexing.typemap.payload", "json")
.option("spark.indextables.indexing.json.parseOnWrite", "true")  // Default: true
.option("spark.indextables.indexing.json.failOnInvalidJson", "false")  // Default: false
```

#### Schema Mapping

```scala
// Spark StringType with json typemap → tantivy4java JSON field
StructField("payload", StringType)
// + configuration: spark.indextables.indexing.typemap.payload = json

// Maps to:
Field payloadField = builder.addJsonField("payload", JsonObjectOptions.storedAndIndexed());
```

#### Write Path Processing

```scala
// Parse JSON string and store as JSON object
def processJsonStringField(
  value: String,
  field: Field,
  config: JsonStringConfig
): Unit = {
  try {
    // Parse JSON string
    val jsonMap: java.util.Map[String, Object] = parseJson(value)

    // Add as JSON field
    doc.addJson(field, jsonMap)

  } catch {
    case e: JsonParseException =>
      if (config.failOnInvalidJson) {
        throw new RuntimeException(s"Invalid JSON in field ${field.name}: $value", e)
      } else {
        // Fallback: Store as regular string
        doc.addText(field, value)
        logWarning(s"Failed to parse JSON in field ${field.name}, storing as text: $value")
      }
  }
}

// JSON parsing utility
def parseJson(jsonString: String): java.util.Map[String, Object] = {
  import com.fasterxml.jackson.databind.ObjectMapper
  val mapper = new ObjectMapper()
  mapper.readValue(jsonString, classOf[java.util.Map[String, Object]])
}
```

#### Read Path Processing

```scala
// Retrieve JSON object and serialize back to string
def retrieveJsonStringField(doc: Document, fieldName: String): String = {
  val jsonMap = doc.getJsonMap(fieldName)
  if (jsonMap != null) {
    // Serialize back to JSON string for Spark
    serializeToJson(jsonMap)
  } else {
    // Fallback: Try to get as text (for invalid JSON that was stored as text)
    doc.getText(fieldName)
  }
}

// JSON serialization utility
def serializeToJson(jsonMap: java.util.Map[String, Object]): String = {
  import com.fasterxml.jackson.databind.ObjectMapper
  val mapper = new ObjectMapper()
  mapper.writeValueAsString(jsonMap)
}
```

#### IndexQuery Translation

**Existing IndexQuery operator extended for JSON fields:**
```scala
// User query: $"payload" indexquery "user_id:123 AND action:click"

// Translation to tantivy4java:
// Parse IndexQuery syntax to extract JSON path queries
val queryTerms = parseIndexQuery("user_id:123 AND action:click")
// Result: List(("user_id", "123", AND), ("action", "click", AND))

// Build tantivy4java JSON queries
val queries = queryTerms.map { case (path, value, occur) =>
  Query.jsonTermQuery(schema, "payload", path, value)
}

// Combine with boolean query
Query combined = Query.booleanQuery(queries, occurs)
```

### JSON String Type Behavior

| Operation | Behavior |
|-----------|----------|
| **Write** | Parse JSON string → Store as JSON object |
| **Write (invalid JSON)** | Store as text (if failOnInvalidJson=false) |
| **Read** | Serialize JSON object → Return as string |
| **IndexQuery** | Parse query → Build JSON term/range queries |
| **Standard Filters** | Treated as text (fallback to Spark) |

### Comparison: String vs Text vs JSON

| Type | Storage | Exact Match Filter | Text Search | JSON Queries | Use Case |
|------|---------|-------------------|-------------|--------------|----------|
| `string` | Raw string | ✅ Pushdown | ❌ | ❌ | IDs, exact values |
| `text` | Tokenized | ⚠️ Partial | ✅ IndexQuery | ❌ | Full-text content |
| `json` | JSON object | ❌ Spark fallback | ✅ IndexQuery | ✅ IndexQuery | Semi-structured data |

---

## Architecture & Implementation

### Component Overview

```
┌─────────────────────────────────────────────────────────────┐
│ IndexTables4Spark (Spark DataSource)                        │
├─────────────────────────────────────────────────────────────┤
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Schema Mapping Layer                                   │  │
│  │ - SparkSchemaToTantivyMapper                          │  │
│  │ - Handles Struct/Array/JSON type detection           │  │
│  │ - Configures JsonObjectOptions                        │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Data Marshalling Layer                                │  │
│  │ - SparkToTantivyConverter (Write)                     │  │
│  │ - TantivyToSparkConverter (Read)                      │  │
│  │ - Handles nested struct/array conversion             │  │
│  │ - JSON string parsing                                 │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
│  ┌───────────────────────────────────────────────────────┐  │
│  │ Predicate Pushdown Layer                              │  │
│  │ - JsonPredicateTranslator                             │  │
│  │ - Converts Spark filters to JSON queries             │  │
│  │ - Handles dot notation, arrays, nested paths         │  │
│  └───────────────────────────────────────────────────────┘  │
│                                                               │
└─────────────────────────────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────┐
│ tantivy4java 0.25.2+                                        │
├─────────────────────────────────────────────────────────────┤
│ - addJsonField(name, JsonObjectOptions)                     │
│ - jsonTermQuery(schema, field, path, value)                │
│ - jsonRangeQuery(schema, field, path, min, max)            │
│ - jsonExistsQuery(schema, field, path)                     │
│ - Document.addJson(field, Map)                             │
│ - Document.getJsonMap(field)                               │
└─────────────────────────────────────────────────────────────┘
```

### Key Classes

#### 1. SparkSchemaToTantivyMapper

```scala
/**
 * Maps Spark schemas to tantivy4java schemas with JSON field support.
 */
class SparkSchemaToTantivyMapper(config: IndexingConfig) {

  /**
   * Determines if a Spark field should be mapped to a JSON field.
   */
  def shouldUseJsonField(field: StructField): Boolean = {
    field.dataType match {
      case _: StructType => true
      case _: ArrayType => true
      case StringType if config.getFieldType(field.name) == "json" => true
      case _ => false
    }
  }

  /**
   * Creates appropriate tantivy4java field for Spark field.
   */
  def createTantivyField(
    builder: SchemaBuilder,
    field: StructField
  ): Field = {
    if (shouldUseJsonField(field)) {
      val options = determineJsonOptions(field)
      builder.addJsonField(field.name, options)
    } else {
      createPrimitiveField(builder, field)
    }
  }

  /**
   * Determines JsonObjectOptions based on field type and configuration.
   */
  def determineJsonOptions(field: StructField): JsonObjectOptions = {
    field.dataType match {
      case st: StructType =>
        // Struct: needs search + retrieval + fast fields for nested queries
        JsonObjectOptions.full()

      case at: ArrayType =>
        // Array: needs search + retrieval + fast fields for element queries
        JsonObjectOptions.full()

      case StringType =>
        // JSON string: configurable, default to indexed + stored
        val options = JsonObjectOptions.storedAndIndexed()
        if (config.requiresRangeQueries(field.name)) {
          options.setFast("raw")
        }
        options
    }
  }
}
```

#### 2. SparkToTantivyConverter

```scala
/**
 * Converts Spark Row data to tantivy4java Document format.
 */
class SparkToTantivyConverter(schema: StructType, config: IndexingConfig) {

  /**
   * Converts an entire Spark Row to tantivy4java Document.
   */
  def convertRow(row: Row, schema: Schema, fields: Map[String, Field]): Document = {
    val doc = new Document()

    schema.fields.zipWithIndex.foreach { case (sparkField, idx) =>
      val value = row.get(idx)
      if (value != null) {
        val tantivyField = fields(sparkField.name)
        addFieldToDocument(doc, tantivyField, value, sparkField)
      }
    }

    doc
  }

  /**
   * Adds a single field to document, handling JSON conversion.
   */
  private def addFieldToDocument(
    doc: Document,
    field: Field,
    value: Any,
    sparkField: StructField
  ): Unit = {
    sparkField.dataType match {
      case st: StructType =>
        val jsonMap = structToJsonMap(value.asInstanceOf[Row], st)
        doc.addJson(field, jsonMap)

      case at: ArrayType =>
        val jsonList = arrayToJsonList(value, at)
        val jsonMap = new java.util.HashMap[String, Object]()
        jsonMap.put("_values", jsonList)  // Wrap array in object
        doc.addJson(field, jsonMap)

      case StringType if config.getFieldType(sparkField.name) == "json" =>
        val jsonMap = parseJsonString(value.asInstanceOf[String])
        doc.addJson(field, jsonMap)

      case _ =>
        addPrimitiveField(doc, field, value, sparkField.dataType)
    }
  }

  /**
   * Converts Spark StructType Row to Java Map.
   */
  def structToJsonMap(row: Row, structType: StructType): java.util.Map[String, Object] = {
    val map = new java.util.HashMap[String, Object]()

    structType.fields.zipWithIndex.foreach { case (field, idx) =>
      val value = row.get(idx)
      if (value != null) {
        val jsonValue = convertToJsonValue(value, field.dataType)
        map.put(field.name, jsonValue)
      }
    }

    map
  }

  /**
   * Converts Spark ArrayType to Java List.
   */
  def arrayToJsonList(value: Any, arrayType: ArrayType): java.util.List[Object] = {
    val sparkSeq = value.asInstanceOf[Seq[_]]
    val javaList = new java.util.ArrayList[Object]()

    sparkSeq.foreach { element =>
      if (element != null) {
        val jsonValue = convertToJsonValue(element, arrayType.elementType)
        javaList.add(jsonValue)
      }
    }

    javaList
  }

  /**
   * Recursively converts Spark value to Java object for JSON.
   */
  private def convertToJsonValue(value: Any, dataType: DataType): Object = {
    dataType match {
      case st: StructType =>
        structToJsonMap(value.asInstanceOf[Row], st)
      case at: ArrayType =>
        arrayToJsonList(value, at)
      case StringType => value.asInstanceOf[String]
      case IntegerType => Integer.valueOf(value.asInstanceOf[Int])
      case LongType => java.lang.Long.valueOf(value.asInstanceOf[Long])
      case FloatType => java.lang.Float.valueOf(value.asInstanceOf[Float])
      case DoubleType => java.lang.Double.valueOf(value.asInstanceOf[Double])
      case BooleanType => java.lang.Boolean.valueOf(value.asInstanceOf[Boolean])
      case DateType =>
        // Convert days since epoch to milliseconds for JSON
        val days = value.asInstanceOf[Int]
        java.lang.Long.valueOf(days.toLong * 86400000L)
      case TimestampType =>
        // Microseconds to milliseconds
        val micros = value.asInstanceOf[Long]
        java.lang.Long.valueOf(micros / 1000L)
      case _ => value.toString  // Fallback
    }
  }

  /**
   * Parses JSON string with error handling.
   */
  private def parseJsonString(jsonStr: String): java.util.Map[String, Object] = {
    try {
      JsonUtils.parseJson(jsonStr)
    } catch {
      case e: Exception =>
        if (config.failOnInvalidJson) {
          throw new RuntimeException(s"Invalid JSON string: $jsonStr", e)
        } else {
          logWarning(s"Failed to parse JSON, storing as text: $jsonStr")
          val map = new java.util.HashMap[String, Object]()
          map.put("_raw", jsonStr)  // Store unparsed string
          map
        }
    }
  }
}
```

#### 3. TantivyToSparkConverter

```scala
/**
 * Converts tantivy4java Document data back to Spark Row format.
 */
class TantivyToSparkConverter(schema: StructType, config: IndexingConfig) {

  /**
   * Converts tantivy4java Document to Spark Row.
   */
  def convertDocument(doc: Document): Row = {
    val values = schema.fields.map { field =>
      if (shouldUseJsonField(field)) {
        retrieveJsonField(doc, field)
      } else {
        retrievePrimitiveField(doc, field)
      }
    }
    Row.fromSeq(values)
  }

  /**
   * Retrieves JSON field and converts to Spark type.
   */
  private def retrieveJsonField(doc: Document, field: StructField): Any = {
    val jsonMap = doc.getJsonMap(field.name)
    if (jsonMap == null) return null

    field.dataType match {
      case st: StructType =>
        jsonMapToRow(jsonMap, st)

      case at: ArrayType =>
        val jsonList = jsonMap.get("_values").asInstanceOf[java.util.List[Object]]
        jsonListToArray(jsonList, at)

      case StringType if config.getFieldType(field.name) == "json" =>
        // Check if this was stored as unparsed text
        if (jsonMap.containsKey("_raw")) {
          jsonMap.get("_raw").asInstanceOf[String]
        } else {
          JsonUtils.serializeToJson(jsonMap)
        }

      case _ => null
    }
  }

  /**
   * Converts Java Map to Spark Row for StructType.
   */
  def jsonMapToRow(
    jsonMap: java.util.Map[String, Object],
    structType: StructType
  ): Row = {
    val values = structType.fields.map { field =>
      val jsonValue = jsonMap.get(field.name)
      if (jsonValue == null) {
        null
      } else {
        convertFromJsonValue(jsonValue, field.dataType)
      }
    }
    Row.fromSeq(values)
  }

  /**
   * Converts Java List to Spark Seq for ArrayType.
   */
  def jsonListToArray(
    jsonList: java.util.List[Object],
    arrayType: ArrayType
  ): Seq[Any] = {
    import scala.jdk.CollectionConverters._

    jsonList.asScala.map { element =>
      convertFromJsonValue(element, arrayType.elementType)
    }.toSeq
  }

  /**
   * Recursively converts JSON value to Spark type.
   */
  private def convertFromJsonValue(jsonValue: Object, dataType: DataType): Any = {
    dataType match {
      case st: StructType =>
        jsonMapToRow(jsonValue.asInstanceOf[java.util.Map[String, Object]], st)
      case at: ArrayType =>
        jsonListToArray(jsonValue.asInstanceOf[java.util.List[Object]], at)
      case StringType => jsonValue.asInstanceOf[String]
      case IntegerType => jsonValue.asInstanceOf[Number].intValue()
      case LongType => jsonValue.asInstanceOf[Number].longValue()
      case FloatType => jsonValue.asInstanceOf[Number].floatValue()
      case DoubleType => jsonValue.asInstanceOf[Number].doubleValue()
      case BooleanType => jsonValue.asInstanceOf[Boolean]
      case DateType =>
        // Milliseconds back to days since epoch
        val millis = jsonValue.asInstanceOf[Number].longValue()
        (millis / 86400000L).toInt
      case TimestampType =>
        // Milliseconds to microseconds
        val millis = jsonValue.asInstanceOf[Number].longValue()
        millis * 1000L
      case _ => jsonValue
    }
  }
}
```

#### 4. JsonPredicateTranslator

```scala
/**
 * Translates Spark Catalyst filters to tantivy4java JSON queries.
 */
class JsonPredicateTranslator(schema: StructType, config: IndexingConfig) {

  /**
   * Translates a Spark filter to tantivy4java Query.
   */
  def translateFilter(filter: Filter, tantivySchema: Schema): Option[Query] = {
    filter match {
      // Nested field equality: $"user.name" === "Alice"
      case EqualTo(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        Some(Query.jsonTermQuery(tantivySchema, fieldName, path, valueToString(value)))

      // Nested field range: $"user.age" > 25
      case GreaterThan(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, numValue, null, false, true))

      case GreaterThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, numValue, null, true, true))

      case LessThan(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, null, numValue, true, false))

      case LessThanOrEqual(attr, value) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        val numValue = valueToLong(value)
        Some(Query.jsonRangeQuery(tantivySchema, fieldName, path, null, numValue, true, true))

      // Field existence: $"user.email".isNotNull
      case IsNotNull(attr) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        Some(Query.jsonExistsQuery(tantivySchema, fieldName, path))

      case IsNull(attr) if isNestedAttribute(attr) =>
        val (fieldName, path) = splitNestedAttribute(attr)
        val existsQuery = Query.jsonExistsQuery(tantivySchema, fieldName, path)
        Some(Query.booleanQuery(
          java.util.Arrays.asList(existsQuery),
          java.util.Arrays.asList(Occur.MUST_NOT)
        ))

      // Array contains: array_contains($"tags", "laptop")
      case ArrayContains(attr, value) =>
        val fieldType = getFieldType(attr)
        fieldType match {
          case at: ArrayType =>
            // Array stored as JSON field
            Some(Query.jsonTermQuery(tantivySchema, attr, "", valueToString(value)))
          case _ => None
        }

      // Boolean combinations
      case And(left, right) =>
        for {
          leftQuery <- translateFilter(left, tantivySchema)
          rightQuery <- translateFilter(right, tantivySchema)
        } yield Query.booleanQuery(
          java.util.Arrays.asList(leftQuery, rightQuery),
          java.util.Arrays.asList(Occur.MUST, Occur.MUST)
        )

      case Or(left, right) =>
        for {
          leftQuery <- translateFilter(left, tantivySchema)
          rightQuery <- translateFilter(right, tantivySchema)
        } yield Query.booleanQuery(
          java.util.Arrays.asList(leftQuery, rightQuery),
          java.util.Arrays.asList(Occur.SHOULD, Occur.SHOULD)
        )

      case Not(child) =>
        translateFilter(child, tantivySchema).map { childQuery =>
          Query.booleanQuery(
            java.util.Arrays.asList(childQuery),
            java.util.Arrays.asList(Occur.MUST_NOT)
          )
        }

      case _ => None  // Not supported, fallback to Spark
    }
  }

  /**
   * Checks if attribute references nested field (contains dot).
   */
  private def isNestedAttribute(attr: String): Boolean = {
    attr.contains(".") && {
      val rootField = attr.split("\\.")(0)
      val field = schema.find(_.name == rootField)
      field.exists(f => f.dataType.isInstanceOf[StructType] || f.dataType.isInstanceOf[ArrayType])
    }
  }

  /**
   * Splits nested attribute into field name and JSON path.
   * Example: "user.address.city" -> ("user", "address.city")
   */
  private def splitNestedAttribute(attr: String): (String, String) = {
    val parts = attr.split("\\.", 2)
    if (parts.length == 2) {
      (parts(0), parts(1))
    } else {
      (parts(0), "")
    }
  }

  /**
   * Gets Spark field type for attribute name.
   */
  private def getFieldType(attr: String): DataType = {
    val rootField = if (attr.contains(".")) attr.split("\\.")(0) else attr
    schema.find(_.name == rootField).map(_.dataType).orNull
  }
}
```

---

## Configuration & API Design

### Configuration Keys

```scala
// Existing configuration (remains unchanged)
"spark.indextables.indexing.typemap.<field>" -> "string" | "text"

// NEW: JSON type for Struct/Array/JSON-string fields
"spark.indextables.indexing.typemap.<field>" -> "json"

// NEW: JSON-specific configuration
"spark.indextables.indexing.json.parseOnWrite" -> "true" | "false"  // Default: true
"spark.indextables.indexing.json.failOnInvalidJson" -> "true" | "false"  // Default: false
"spark.indextables.indexing.json.enableRangeQueries" -> "true" | "false"  // Default: true
```

### Automatic JSON Type Detection

**Struct and Array types are automatically detected:**
```scala
// No configuration needed for Struct/Array - automatic!
val schema = StructType(Seq(
  StructField("id", StringType),
  StructField("user", StructType(...)),  // Automatically uses JSON
  StructField("tags", ArrayType(StringType))  // Automatically uses JSON
))

df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/path")  // Just works!
```

**String-to-JSON requires explicit configuration:**
```scala
// Explicit configuration required for JSON string parsing
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexing.typemap.payload", "json")
  .save("s3://bucket/path")
```

### Type Mapping Summary

| Spark Type | Configuration | tantivy4java Field | Notes |
|------------|---------------|-------------------|-------|
| `StructType` | Automatic | `addJsonField()` | No config needed |
| `ArrayType` | Automatic | `addJsonField()` | No config needed |
| `StringType` | `typemap.<field>=json` | `addJsonField()` | Explicit opt-in |
| `StringType` | `typemap.<field>=string` | `addTextField()` (raw) | Exact match |
| `StringType` | `typemap.<field>=text` | `addTextField()` (tokenized) | Full-text |

---

## Predicate Pushdown Strategy

### Filter Classification

Filters are classified into three categories:

1. **Full Pushdown**: Executed entirely in tantivy4java, no Spark post-processing
2. **Partial Pushdown**: Conservative query in tantivy4java + Spark refinement
3. **No Pushdown**: Handled entirely by Spark

### Pushdown Support Matrix

#### Struct Field Filters

| Filter Type | Example | Pushdown | tantivy4java Query |
|-------------|---------|----------|-------------------|
| Equality | `$"user.name" === "Alice"` | ✅ Full | `jsonTermQuery("user", "name", "Alice")` |
| Greater than | `$"user.age" > 25` | ✅ Full | `jsonRangeQuery("user", "age", 26, null)` |
| Less than | `$"user.age" < 30` | ✅ Full | `jsonRangeQuery("user", "age", null, 29)` |
| Between | `$"user.age" > 25 && $"user.age" < 30` | ✅ Full | `jsonRangeQuery("user", "age", 26, 29)` |
| IsNotNull | `$"user.email".isNotNull` | ✅ Full | `jsonExistsQuery("user", "email")` |
| IsNull | `$"user.email".isNull` | ✅ Full | `NOT jsonExistsQuery("user", "email")` |
| StartsWith | `$"user.name".startsWith("Ali")` | ❌ None | Spark filter |
| Contains | `$"user.name".contains("lic")` | ❌ None | Spark filter |

#### Array Field Filters

| Filter Type | Example | Pushdown | tantivy4java Query |
|-------------|---------|----------|-------------------|
| array_contains | `array_contains($"tags", "laptop")` | ✅ Full | `jsonTermQuery("tags", "", "laptop")` |
| Nested field | `$"reviews.rating" > 4` | ✅ Full | `jsonRangeQuery("reviews", "rating", 5, null)` |
| Array size | `size($"tags") > 3` | ❌ None | Spark filter |
| Array index | `$"tags"[0] === "electronics"` | ⚠️ Partial | `jsonTermQuery("tags", "", "electronics")` + Spark |

#### JSON String Field Filters

| Filter Type | Example | Pushdown | tantivy4java Query |
|-------------|---------|----------|-------------------|
| IndexQuery | `$"payload" indexquery "user_id:123"` | ✅ Full | `jsonTermQuery("payload", "user_id", "123")` |
| IndexQuery range | `$"payload" indexquery "age:[25 TO 30]"` | ✅ Full | `jsonRangeQuery("payload", "age", 25, 30)` |
| Equality | `$"payload" === "{...}"` | ❌ None | Spark filter (not useful) |

### Pushdown Decision Logic

```scala
def shouldPushdown(filter: Filter): Boolean = {
  filter match {
    // Struct field filters
    case EqualTo(attr, _) if isNestedStruct(attr) => true
    case GreaterThan(attr, _) if isNestedStruct(attr) => true
    case GreaterThanOrEqual(attr, _) if isNestedStruct(attr) => true
    case LessThan(attr, _) if isNestedStruct(attr) => true
    case LessThanOrEqual(attr, _) if isNestedStruct(attr) => true
    case IsNotNull(attr) if isNestedStruct(attr) => true
    case IsNull(attr) if isNestedStruct(attr) => true

    // Array filters
    case ArrayContains(attr, _) if isArray(attr) => true
    case EqualTo(attr, _) if isNestedArray(attr) => true  // reviews.rating
    case GreaterThan(attr, _) if isNestedArray(attr) => true
    case GreaterThanOrEqual(attr, _) if isNestedArray(attr) => true
    case LessThan(attr, _) if isNestedArray(attr) => true
    case LessThanOrEqual(attr, _) if isNestedArray(attr) => true

    // Boolean combinations (recursive)
    case And(left, right) => shouldPushdown(left) && shouldPushdown(right)
    case Or(left, right) => shouldPushdown(left) && shouldPushdown(right)
    case Not(child) => shouldPushdown(child)

    // Not supported
    case _ => false
  }
}
```

---

## Test Plan

### Test Coverage Goals

- **Unit Tests**: 50+ tests covering all conversion logic
- **Integration Tests**: 30+ tests for end-to-end workflows
- **Performance Tests**: 10+ tests measuring predicate pushdown efficiency
- **Edge Case Tests**: 20+ tests for error conditions and boundary cases

### Test Organization

```
src/test/scala/io/indextables/spark/json/
├── StructTypeTest.scala               (Struct field tests)
├── ArrayTypeTest.scala                (Array field tests)
├── JsonStringTypeTest.scala           (JSON string field tests)
├── JsonPredicatePushdownTest.scala    (Pushdown logic tests)
├── JsonSchemaConversionTest.scala     (Schema mapping tests)
├── JsonDataConversionTest.scala       (Data marshalling tests)
└── JsonEndToEndTest.scala             (Full integration tests)
```

### Test Cases: Struct Type Support

#### StructTypeTest.scala

```scala
class StructTypeTest extends QueryTest with SharedSparkSession {

  test("write and read simple struct") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30)),
      ("2", Row("Bob", 25))
    )).toDF("id", "user")

    val path = s"$tempDir/simple_struct"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("write and read nested struct (3 levels)") {
    case class Address(city: String, zip: String, coords: Row)
    case class User(name: String, age: Int, address: Address)

    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30, Row("NYC", "10001", Row(40.7128, -74.0060)))),
      ("2", Row("Bob", 25, Row("SF", "94102", Row(37.7749, -122.4194))))
    )).toDF("id", "user")

    val path = s"$tempDir/nested_struct"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("struct with null fields") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", null, Row("NYC", null))),
      ("2", Row(null, 25, Row(null, "94102")))
    )).toDF("id", "user")

    val path = s"$tempDir/struct_nulls"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("struct with all supported primitive types") {
    val schema = StructType(Seq(
      StructField("id", StringType),
      StructField("data", StructType(Seq(
        StructField("str", StringType),
        StructField("int", IntegerType),
        StructField("long", LongType),
        StructField("float", FloatType),
        StructField("double", DoubleType),
        StructField("bool", BooleanType),
        StructField("date", DateType),
        StructField("timestamp", TimestampType)
      )))
    ))

    val df = spark.createDataFrame(
      spark.sparkContext.parallelize(Seq(
        Row("1", Row("test", 42, 9876543210L, 3.14f, 2.718, true,
          java.sql.Date.valueOf("2024-01-15"),
          java.sql.Timestamp.valueOf("2024-01-15 10:30:00")))
      )),
      schema
    )

    val path = s"$tempDir/struct_all_types"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("predicate pushdown: struct field equality") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30)),
      ("2", Row("Bob", 25)),
      ("3", Row("Charlie", 35))
    )).toDF("id", "user")

    val path = s"$tempDir/pushdown_equality"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.name" === "Alice")

    checkAnswer(result, Seq(Row("1", Row("Alice", 30))))

    // Verify pushdown occurred
    val plan = result.queryExecution.executedPlan
    assert(plan.toString.contains("PushedFilters"))
  }

  test("predicate pushdown: struct field range query") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30)),
      ("2", Row("Bob", 25)),
      ("3", Row("Charlie", 35))
    )).toDF("id", "user")

    val path = s"$tempDir/pushdown_range"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.age" > 25 && $"user.age" < 35)

    checkAnswer(result, Seq(Row("1", Row("Alice", 30))))
  }

  test("predicate pushdown: nested struct field (3 levels)") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", Row("NYC", Row(40.7, -74.0)))),
      ("2", Row("Bob", Row("SF", Row(37.7, -122.4)))),
      ("3", Row("Charlie", Row("LA", Row(34.0, -118.2))))
    )).toDF("id", "user")

    val path = s"$tempDir/pushdown_deep_nested"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.address.city" === "NYC")

    checkAnswer(result, Seq(Row("1", Row("Alice", Row("NYC", Row(40.7, -74.0))))))
  }

  test("predicate pushdown: IsNotNull on struct field") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", "alice@example.com")),
      ("2", Row("Bob", null)),
      ("3", Row("Charlie", "charlie@example.com"))
    )).toDF("id", "user")

    val path = s"$tempDir/pushdown_isnotnull"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.email".isNotNull)

    checkAnswer(result, Seq(
      Row("1", Row("Alice", "alice@example.com")),
      Row("3", Row("Charlie", "charlie@example.com"))
    ))
  }

  test("predicate pushdown: complex boolean combinations") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30, "NYC")),
      ("2", Row("Bob", 25, "SF")),
      ("3", Row("Charlie", 35, "NYC")),
      ("4", Row("David", 28, "LA"))
    )).toDF("id", "user")

    val path = s"$tempDir/pushdown_boolean"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(
        ($"user.age" > 25 && $"user.city" === "NYC") ||
        ($"user.name" === "Bob")
      )

    checkAnswer(result, Seq(
      Row("1", Row("Alice", 30, "NYC")),
      Row("2", Row("Bob", 25, "SF")),
      Row("3", Row("Charlie", 35, "NYC"))
    ))
  }

  test("struct with mixed JSON and primitive fields") {
    val df = spark.createDataFrame(Seq(
      ("1", "Product A", Row("electronics", 299.99)),
      ("2", "Product B", Row("books", 19.99))
    )).toDF("id", "name", "info")

    val path = s"$tempDir/mixed_fields"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.name", "string")
      .save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"name" === "Product A" && $"info.category" === "electronics")

    checkAnswer(result, Seq(Row("1", "Product A", Row("electronics", 299.99))))
  }

  test("error: unsupported operation on struct should use Spark fallback") {
    val df = spark.createDataFrame(Seq(
      ("1", Row("Alice", 30)),
      ("2", Row("Bob", 25))
    )).toDF("id", "user")

    val path = s"$tempDir/unsupported_struct_op"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.name".startsWith("Ali"))  // Not pushed down

    checkAnswer(result, Seq(Row("1", Row("Alice", 30))))

    // Verify no pushdown for unsupported operation
    val plan = result.queryExecution.executedPlan
    // Should see post-filter in Spark plan
  }
}
```

### Test Cases: Array Type Support

#### ArrayTypeTest.scala

```scala
class ArrayTypeTest extends QueryTest with SharedSparkSession {

  test("write and read simple string array") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("tag1", "tag2", "tag3")),
      ("2", Seq("tag4", "tag5"))
    )).toDF("id", "tags")

    val path = s"$tempDir/simple_array"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("write and read array of structs") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq(Row("Alice", 5), Row("Bob", 4))),
      ("2", Seq(Row("Charlie", 3)))
    )).toDF("id", "reviews")

    val path = s"$tempDir/array_of_structs"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("write and read nested arrays (2D array)") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq(Seq(1, 2), Seq(3, 4))),
      ("2", Seq(Seq(5), Seq(6, 7, 8)))
    )).toDF("id", "matrix")

    val path = s"$tempDir/nested_arrays"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("array with null elements") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a", null, "c")),
      ("2", Seq(null, "d"))
    )).toDF("id", "values")

    val path = s"$tempDir/array_nulls"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("array of all primitive types") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq(1, 2, 3), Seq(1.1, 2.2), Seq(true, false)),
      ("2", Seq(4, 5), Seq(3.3, 4.4, 5.5), Seq(false))
    )).toDF("id", "ints", "doubles", "bools")

    val path = s"$tempDir/array_primitives"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("predicate pushdown: array_contains") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("laptop", "electronics", "portable")),
      ("2", Seq("book", "fiction")),
      ("3", Seq("laptop", "gaming"))
    )).toDF("id", "tags")

    val path = s"$tempDir/pushdown_array_contains"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(array_contains($"tags", "laptop"))

    checkAnswer(result, Seq(
      Row("1", Seq("laptop", "electronics", "portable")),
      Row("3", Seq("laptop", "gaming"))
    ))

    // Verify pushdown
    val plan = result.queryExecution.executedPlan
    assert(plan.toString.contains("PushedFilters"))
  }

  test("predicate pushdown: nested array struct field") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq(Row("Alice", 5), Row("Bob", 4))),
      ("2", Seq(Row("Charlie", 3), Row("David", 5))),
      ("3", Seq(Row("Eve", 2)))
    )).toDF("id", "reviews")

    val path = s"$tempDir/pushdown_array_struct"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"reviews.rating" >= 5)  // Any review with rating >= 5

    checkAnswer(result, Seq(
      Row("1", Seq(Row("Alice", 5), Row("Bob", 4))),
      Row("2", Seq(Row("Charlie", 3), Row("David", 5)))
    ))
  }

  test("predicate pushdown: array numeric range") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq(10, 20, 30)),
      ("2", Seq(5, 15, 25)),
      ("3", Seq(1, 2, 3))
    )).toDF("id", "values")

    val path = s"$tempDir/pushdown_array_range"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"values" >= 20)  // Any value >= 20

    checkAnswer(result, Seq(
      Row("1", Seq(10, 20, 30)),
      Row("2", Seq(5, 15, 25))
    ))
  }

  test("array_contains with multiple conditions") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("laptop", "electronics"), 500.0),
      ("2", Seq("book", "fiction"), 20.0),
      ("3", Seq("laptop", "gaming"), 1200.0)
    )).toDF("id", "tags", "price")

    val path = s"$tempDir/array_multiple_conditions"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(array_contains($"tags", "laptop") && $"price" < 1000)

    checkAnswer(result, Seq(
      Row("1", Seq("laptop", "electronics"), 500.0)
    ))
  }

  test("empty arrays") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a", "b")),
      ("2", Seq()),
      ("3", Seq("c"))
    )).toDF("id", "tags")

    val path = s"$tempDir/empty_arrays"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("error: size() function not pushed down") {
    val df = spark.createDataFrame(Seq(
      ("1", Seq("a", "b", "c")),
      ("2", Seq("d"))
    )).toDF("id", "tags")

    val path = s"$tempDir/array_size"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(size($"tags") > 1)  // Not pushed down, Spark fallback

    checkAnswer(result, Seq(Row("1", Seq("a", "b", "c"))))
  }
}
```

### Test Cases: JSON String Type Support

#### JsonStringTypeTest.scala

```scala
class JsonStringTypeTest extends QueryTest with SharedSparkSession {

  test("write and read JSON string field") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"user_id": "u123", "action": "click"}"""),
      ("2", """{"user_id": "u456", "action": "view"}""")
    )).toDF("id", "payload")

    val path = s"$tempDir/json_string"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.payload", "json")
      .save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("JSON string with nested objects") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"user": {"id": "u123", "name": "Alice"}, "timestamp": 1234567890}"""),
      ("2", """{"user": {"id": "u456", "name": "Bob"}, "timestamp": 1234567891}""")
    )).toDF("id", "event")

    val path = s"$tempDir/json_nested"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.event", "json")
      .save(path)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    checkAnswer(result, df)
  }

  test("IndexQuery on JSON string field") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"user_id": "u123", "action": "click"}"""),
      ("2", """{"user_id": "u456", "action": "view"}"""),
      ("3", """{"user_id": "u123", "action": "view"}""")
    )).toDF("id", "payload")

    val path = s"$tempDir/json_indexquery"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.payload", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"payload" indexquery "user_id:u123")

    checkAnswer(result, Seq(
      Row("1", """{"user_id": "u123", "action": "click"}"""),
      Row("3", """{"user_id": "u123", "action": "view"}""")
    ))
  }

  test("IndexQuery with nested JSON path") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"user": {"id": "u123", "role": "admin"}}"""),
      ("2", """{"user": {"id": "u456", "role": "user"}}""")
    )).toDF("id", "payload")

    val path = s"$tempDir/json_nested_query"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.payload", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"payload" indexquery "user.role:admin")

    checkAnswer(result, Seq(
      Row("1", """{"user": {"id": "u123", "role": "admin"}}""")
    ))
  }

  test("IndexQuery with boolean combinations") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"user_id": "u123", "action": "click", "page": "/home"}"""),
      ("2", """{"user_id": "u456", "action": "view", "page": "/about"}"""),
      ("3", """{"user_id": "u123", "action": "view", "page": "/home"}""")
    )).toDF("id", "event")

    val path = s"$tempDir/json_boolean_query"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.event", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"event" indexquery "user_id:u123 AND page:/home")

    checkAnswer(result, Seq(
      Row("1", """{"user_id": "u123", "action": "click", "page": "/home"}"""),
      Row("3", """{"user_id": "u123", "action": "view", "page": "/home"}""")
    ))
  }

  test("invalid JSON with failOnInvalidJson=false") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"valid": "json"}"""),
      ("2", """invalid json{"""),  // Invalid JSON
      ("3", """{"also": "valid"}""")
    )).toDF("id", "payload")

    val path = s"$tempDir/json_invalid"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.payload", "json")
      .option("spark.indextables.indexing.json.failOnInvalidJson", "false")
      .save(path)

    // Should succeed, storing invalid JSON as text
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
    assert(result.count() == 3)
  }

  test("invalid JSON with failOnInvalidJson=true") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"valid": "json"}"""),
      ("2", """invalid json{""")  // Invalid JSON
    )).toDF("id", "payload")

    val path = s"$tempDir/json_fail_on_invalid"

    // Should throw exception
    intercept[Exception] {
      df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
        .option("spark.indextables.indexing.typemap.payload", "json")
        .option("spark.indextables.indexing.json.failOnInvalidJson", "true")
        .save(path)
    }
  }

  test("JSON string with arrays") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"tags": ["laptop", "electronics"]}"""),
      ("2", """{"tags": ["book", "fiction"]}""")
    )).toDF("id", "data")

    val path = s"$tempDir/json_with_arrays"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.data", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"data" indexquery "tags:laptop")

    checkAnswer(result, Seq(
      Row("1", """{"tags": ["laptop", "electronics"]}""")
    ))
  }

  test("JSON string range query") {
    val df = spark.createDataFrame(Seq(
      ("1", """{"price": 100, "category": "electronics"}"""),
      ("2", """{"price": 500, "category": "electronics"}"""),
      ("3", """{"price": 1000, "category": "electronics"}""")
    )).toDF("id", "product")

    val path = s"$tempDir/json_range"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.product", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"product" indexquery "price:[100 TO 500]")

    checkAnswer(result, Seq(
      Row("1", """{"price": 100, "category": "electronics"}"""),
      Row("2", """{"price": 500, "category": "electronics"}""")
    ))
  }

  test("mix JSON string field with regular fields") {
    val df = spark.createDataFrame(Seq(
      ("1", "Title 1", """{"author": "Alice", "year": 2024}"""),
      ("2", "Title 2", """{"author": "Bob", "year": 2023}""")
    )).toDF("id", "title", "metadata")

    val path = s"$tempDir/json_mixed"
    df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .option("spark.indextables.indexing.typemap.title", "string")
      .option("spark.indextables.indexing.typemap.metadata", "json")
      .save(path)

    import org.apache.spark.sql.indextables.IndexQueryExpression._
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"title" === "Title 1" && ($"metadata" indexquery "author:Alice"))

    checkAnswer(result, Seq(
      Row("1", "Title 1", """{"author": "Alice", "year": 2024}""")
    ))
  }
}
```

### Test Cases: Predicate Pushdown Validation

#### JsonPredicatePushdownTest.scala

```scala
class JsonPredicatePushdownTest extends QueryTest with SharedSparkSession {

  test("verify struct equality pushdown") {
    val df = createStructDataFrame()
    val path = writeTempTable(df)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.name" === "Alice")

    // Verify pushdown in plan
    val plan = result.queryExecution.executedPlan.toString
    assert(plan.contains("PushedFilters: [IsNotNull(user.name), EqualTo(user.name,Alice)]"))

    // Verify correct results
    checkAnswer(result, df.filter($"user.name" === "Alice"))
  }

  test("verify struct range pushdown") {
    val df = createStructDataFrame()
    val path = writeTempTable(df)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.age" > 25 && $"user.age" < 35)

    val plan = result.queryExecution.executedPlan.toString
    assert(plan.contains("PushedFilters"))

    checkAnswer(result, df.filter($"user.age" > 25 && $"user.age" < 35))
  }

  test("verify array_contains pushdown") {
    val df = createArrayDataFrame()
    val path = writeTempTable(df)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(array_contains($"tags", "laptop"))

    val plan = result.queryExecution.executedPlan.toString
    assert(plan.contains("PushedFilters"))

    checkAnswer(result, df.filter(array_contains($"tags", "laptop")))
  }

  test("verify no pushdown for unsupported operations") {
    val df = createStructDataFrame()
    val path = writeTempTable(df)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.name".startsWith("Ali"))

    // Should NOT have pushdown for startsWith
    val plan = result.queryExecution.executedPlan.toString
    assert(!plan.contains("PushedFilters.*startsWith"))

    checkAnswer(result, df.filter($"user.name".startsWith("Ali")))
  }

  test("verify complex boolean pushdown") {
    val df = createComplexDataFrame()
    val path = writeTempTable(df)

    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(
        ($"user.age" > 25 && $"user.city" === "NYC") ||
        array_contains($"tags", "premium")
      )

    val plan = result.queryExecution.executedPlan.toString
    assert(plan.contains("PushedFilters"))

    // Verify correct results with complex filter
    val expected = df.filter(
      ($"user.age" > 25 && $"user.city" === "NYC") ||
      array_contains($"tags", "premium")
    )
    checkAnswer(result, expected)
  }
}
```

### Performance Test Cases

```scala
class JsonPerformanceTest extends QueryTest with SharedSparkSession {

  test("struct pushdown performance: 1M records") {
    val df = spark.range(1000000).map { i =>
      (i.toString, Row(s"user_$i", (i % 100).toInt, s"city_${i % 10}"))
    }.toDF("id", "user")

    val path = writeTempTable(df)

    // Measure pushdown query
    val start = System.currentTimeMillis()
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.city" === "city_5")
      .count()
    val pushdownTime = System.currentTimeMillis() - start

    // Measure Spark-only filtering (disable pushdown)
    val start2 = System.currentTimeMillis()
    val result2 = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter($"user.name".startsWith("user_5"))  // Not pushed down
      .count()
    val sparkOnlyTime = System.currentTimeMillis() - start2

    assert(result == 100000)  // 10% of records match

    logInfo(s"Pushdown time: ${pushdownTime}ms")
    logInfo(s"Spark-only time: ${sparkOnlyTime}ms")
    logInfo(s"Speedup: ${sparkOnlyTime.toDouble / pushdownTime}x")

    // Pushdown should be significantly faster
    assert(pushdownTime < sparkOnlyTime * 0.5)  // At least 2x faster
  }

  test("array_contains performance: 1M records with large arrays") {
    val df = spark.range(1000000).map { i =>
      val tags = (0 until 10).map(j => s"tag_${(i + j) % 100}")
      (i.toString, tags)
    }.toDF("id", "tags")

    val path = writeTempTable(df)

    val start = System.currentTimeMillis()
    val result = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load(path)
      .filter(array_contains($"tags", "tag_50"))
      .count()
    val queryTime = System.currentTimeMillis() - start

    logInfo(s"array_contains query time: ${queryTime}ms for ${result} matches")

    // Should complete in reasonable time
    assert(queryTime < 5000)  // < 5 seconds for 1M records
  }
}
```

---

## Migration & Backward Compatibility

### Backward Compatibility Strategy

**Goal**: Existing tables continue to work without modification. New tables automatically support complex types.

### Schema Evolution

**Existing Tables (Pre-JSON):**
```scala
// Table written before JSON support
df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/old-table")
// Schema: only primitive types, no Struct/Array

// Reading works unchanged
val oldData = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/old-table")
// ✅ No changes needed, existing functionality preserved
```

**New Tables (Post-JSON):**
```scala
// New table with Struct/Array types
val newDf = createComplexDataFrame()  // Contains Struct and Array fields
newDf.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider").save("s3://bucket/new-table")

val newData = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider").load("s3://bucket/new-table")
// ✅ Struct and Array fields work seamlessly
```

### Schema Compatibility Check

```scala
/**
 * Validates schema compatibility for existing tables.
 */
class SchemaCompatibilityChecker {

  def validateSchemaEvolution(
    existingSchema: Schema,
    newSchema: StructType
  ): ValidationResult = {

    val results = newSchema.fields.map { newField =>
      val existingField = existingSchema.getField(newField.name)

      if (existingField == null) {
        // New field: allowed
        ValidationResult.Success
      } else {
        validateFieldTypeChange(existingField, newField)
      }
    }

    ValidationResult.combine(results)
  }

  private def validateFieldTypeChange(
    existingField: Field,
    newField: StructField
  ): ValidationResult = {
    (existingField.fieldType, newField.dataType) match {
      // Primitive to JSON: not allowed (breaking change)
      case (FieldType.Text, _: StructType) =>
        ValidationResult.Error("Cannot change primitive field to Struct")

      case (FieldType.Text, _: ArrayType) =>
        ValidationResult.Error("Cannot change primitive field to Array")

      // Same type: allowed
      case _ if typesCompatible(existingField, newField) =>
        ValidationResult.Success

      // Incompatible types
      case _ =>
        ValidationResult.Error(s"Incompatible type change for field ${newField.name}")
    }
  }
}
```

### Migration Path for Adding Complex Types

**Scenario**: Existing table with string field, want to convert to JSON for querying

**Option 1: Create New Field (Recommended)**
```scala
// Existing table
// Schema: id: String, data: String

// Add new JSON-typed field alongside existing string field
val enrichedDf = existingDf.withColumn("data_json",
  parse_json($"data").cast(StructType(...)))

enrichedDf.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .option("spark.indextables.indexing.typemap.data", "string")  // Keep original
  .option("spark.indextables.indexing.typemap.data_json", "json")  // New JSON field
  .mode("overwrite")
  .save("s3://bucket/enriched-table")
```

**Option 2: Rewrite Table (Breaking Change)**
```scala
// Rewrite entire table with new schema
val newDf = existingDf.select(
  $"id",
  parse_json($"data").cast(StructType(...)).as("data")
)

newDf.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
  .mode("overwrite")
  .save("s3://bucket/rewritten-table")
```

---

## Performance Considerations

### Storage Overhead

**JSON Field Storage:**
- **Struct fields**: Similar overhead to equivalent flat fields (~5-10% due to JSON structure)
- **Array fields**: Comparable to repeated flat fields
- **JSON string fields**: Parsed JSON has slight overhead vs. raw string (~10-15%)

**Example Comparison:**
```scala
// Flat schema storage
case class FlatRecord(id: String, user_name: String, user_age: Int, user_city: String)
// Estimated size: ~100 bytes per record

// Nested schema storage
case class NestedRecord(id: String, user: User)
case class User(name: String, age: Int, city: String)
// Estimated size: ~110 bytes per record (10% overhead)
```

### Query Performance

**Predicate Pushdown Efficiency:**
- **Struct field queries**: Same performance as flat field queries (native JSON query support)
- **Array contains**: High performance with JSON term queries
- **Range queries on nested fields**: Requires fast fields (configured automatically)

**Performance Comparison:**
```scala
// Flat schema query
df.filter($"user_city" === "NYC")
// Scan time: 100ms (baseline)

// Nested schema query with pushdown
df.filter($"user.city" === "NYC")
// Scan time: 105ms (~5% overhead for JSON path resolution)

// Without pushdown (Spark fallback)
df.filter($"user.city" === "NYC")  // If pushdown disabled
// Scan time: 1500ms (15x slower - all data pulled through Spark)
```

### Optimization Recommendations

1. **Enable Fast Fields for Range Queries:**
   ```scala
   // Automatic for Struct/Array types
   // No configuration needed - handled by SparkSchemaToTantivyMapper
   ```

2. **Use Partitioning with Nested Fields:**
   ```scala
   // Partition by top-level or nested field
   df.write.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
     .partitionBy("user.city")  // Nested partitioning supported
     .save("s3://bucket/partitioned")
   ```

3. **Selective Field Projection:**
   ```scala
   // Project only needed nested fields to reduce I/O
   df.select($"id", $"user.name", $"user.age")
     .filter($"user.city" === "NYC")
   ```

4. **Cache Strategy for JSON Fields:**
   ```scala
   // JSON fields use same cache infrastructure
   .option("spark.indextables.cache.maxSize", "500000000")  // 500MB cache
   ```

### Memory Considerations

**Write Path:**
- JSON conversion happens in executor memory
- Memory usage scales with batch size: `batchSize * avgRecordSize`
- Recommendation: Keep default batch size (10000) for complex nested structures

**Read Path:**
- JSON deserialization uses streaming approach (low memory footprint)
- Document retrieval memory: `~10KB per document` (typical)

---

## Implementation Roadmap

### Phase 1: Core Infrastructure (Weeks 1-2)

**Week 1: Schema & Conversion Layer**
- [ ] Implement `SparkSchemaToTantivyMapper`
  - Struct type detection
  - Array type detection
  - JSON string type configuration
  - `JsonObjectOptions` determination logic
- [ ] Implement `SparkToTantivyConverter`
  - `structToJsonMap()` method
  - `arrayToJsonList()` method
  - `parseJsonString()` method with error handling
  - Recursive type conversion
- [ ] Implement `TantivyToSparkConverter`
  - `jsonMapToRow()` method
  - `jsonListToArray()` method
  - `serializeToJson()` for JSON strings
  - Type conversion utilities
- [ ] Unit tests for conversion logic (20 tests)

**Week 2: Predicate Pushdown**
- [ ] Implement `JsonPredicateTranslator`
  - Nested attribute detection
  - Struct field filter translation
  - Array filter translation (array_contains)
  - Boolean combination handling
- [ ] Extend existing filter pushdown infrastructure
  - Integrate JSON predicate translator
  - Update `supportsPushDownFilters()` logic
  - Handle mixed JSON and primitive filters
- [ ] Unit tests for predicate translation (15 tests)

### Phase 2: Integration & Testing (Weeks 3-4)

**Week 3: Integration**
- [ ] Integrate schema mapper into write path
  - Update `IndexTables4SparkWriter`
  - Add JSON field creation logic
  - Configure `JsonObjectOptions` per field
- [ ] Integrate converters into write path
  - Document creation with JSON fields
  - Batch processing for JSON conversion
  - Error handling for invalid JSON strings
- [ ] Integrate converters into read path
  - Document retrieval with JSON deserialization
  - Row construction from JSON data
- [ ] Integration tests (20 tests)

**Week 4: Comprehensive Testing**
- [ ] Struct type tests (12 tests)
- [ ] Array type tests (10 tests)
- [ ] JSON string type tests (10 tests)
- [ ] Predicate pushdown validation tests (8 tests)
- [ ] Performance tests (5 tests)
- [ ] Edge case and error handling tests (10 tests)

### Phase 3: Advanced Features & Optimization (Week 5)

- [ ] Partitioned dataset support for nested fields
  - Partition column extraction from nested paths
  - Partition pruning with nested predicates
- [ ] Aggregate pushdown for nested fields
  - COUNT/SUM/AVG on nested numeric fields
  - MIN/MAX on nested fields
- [ ] Cache optimization for JSON fields
  - Schema-based cache key generation
  - Efficient JSON field caching
- [ ] Advanced tests (15 tests)

### Phase 4: Documentation & Release (Week 6)

- [ ] Update CLAUDE.md with JSON capabilities
  - Configuration options
  - Usage examples
  - Performance guidelines
- [ ] Create user guide examples
  - Struct type examples
  - Array type examples
  - JSON string examples
- [ ] Performance benchmarking
  - Comparison: flat vs nested schemas
  - Pushdown vs Spark-only filtering
  - Memory usage profiling
- [ ] Release preparation
  - Version bump to v2.1
  - Changelog update
  - Release notes

### Testing Summary

**Total Test Count: 110+ Tests**
- Struct type tests: 12
- Array type tests: 10
- JSON string type tests: 10
- Predicate pushdown tests: 8
- Integration tests: 20
- Advanced feature tests: 15
- Performance tests: 5
- Edge case tests: 10
- Unit tests (conversion/schema): 20

**Test Coverage Goals:**
- Schema mapping: 100%
- Data conversion: 100%
- Predicate translation: 100%
- Integration: 100%
- Error handling: 100%

---

## Success Criteria

### Feature Completeness

✅ **Struct Type Support:**
- Write Struct columns without errors
- Read Struct columns with correct types
- Predicate pushdown on nested fields (equality, range, exists)
- Null handling in nested structures
- Multi-level nesting (3+ levels)

✅ **Array Type Support:**
- Write Array columns without errors
- Read Array columns with correct types
- array_contains predicate pushdown
- Nested array-of-struct queries
- Null element handling

✅ **JSON String Type Support:**
- Parse JSON strings on write
- Serialize JSON objects on read
- IndexQuery with JSONPath syntax
- Boolean query combinations
- Error handling for invalid JSON

### Performance Criteria

✅ **Query Performance:**
- Predicate pushdown achieves 2-10x speedup vs Spark-only
- Struct field queries within 10% of flat field query time
- Array contains queries scale linearly with data size

✅ **Storage Efficiency:**
- JSON field overhead < 15% vs flat schema
- No excessive index size growth

✅ **Memory Usage:**
- Write path memory usage scales with batch size
- Read path memory usage < 10KB per document

### Quality Criteria

✅ **Test Coverage:**
- 110+ tests covering all scenarios
- All predicate pushdown variants tested
- Performance benchmarks validated
- Edge cases and error conditions covered

✅ **Backward Compatibility:**
- Existing tables read without modification
- No breaking changes to API
- Schema evolution supported

✅ **Documentation:**
- Complete user guide with examples
- Configuration reference updated
- Performance guidelines documented

---

## Risks & Mitigation

### Risk 1: tantivy4java Version Dependency

**Risk**: tantivy4java 0.25.2+ required, may not be available in all environments

**Mitigation**:
- Check tantivy4java version at runtime
- Provide clear error message if version too old
- Document version requirement prominently

### Risk 2: JSON Parsing Performance

**Risk**: Large JSON strings may cause performance issues during writes

**Mitigation**:
- Implement streaming JSON parser for large payloads
- Configurable batch size for JSON conversion
- Option to disable JSON parsing per field

### Risk 3: Complex Nested Query Performance

**Risk**: Deep nested queries (5+ levels) may have poor performance

**Mitigation**:
- Benchmark deep nested queries during development
- Document recommended nesting depth limits
- Provide optimization guidelines for complex schemas

### Risk 4: Array Element Index Access

**Risk**: tantivy4java doesn't support indexed array access (`arr[0]`), may confuse users

**Mitigation**:
- Clearly document "any element matches" behavior
- Provide examples showing expected semantics
- Warn in logs when indexed access used (conservative pushdown)

### Risk 5: Invalid JSON Handling

**Risk**: Users may have inconsistent JSON quality in string fields

**Mitigation**:
- Default to graceful degradation (store as text on parse failure)
- Provide strict mode option (`failOnInvalidJson=true`)
- Log warnings for all parse failures

---

## Open Questions

1. **Array Index Access Semantics:**
   - Should `$"arr"[0] === value` push down as "any element" or not push down at all?
   - **Proposed**: Push down conservatively (any element), document clearly

2. **JSON String Storage Format:**
   - Should we store original string or normalized JSON?
   - **Proposed**: Store normalized JSON for consistency, original lost

3. **Schema Evolution:**
   - Allow changing primitive field to JSON field?
   - **Proposed**: Disallow (breaking change), require new field or table rewrite

4. **Performance Tuning:**
   - Default `JsonObjectOptions` for different scenarios?
   - **Proposed**: Use `.full()` for all Struct/Array types (safe default)

5. **Null Handling:**
   - How to handle null vs missing keys in JSON objects?
   - **Proposed**: Treat identically (both map to Spark null)

---

## Conclusion

This design provides comprehensive JSON field integration for IndexTables4Spark, enabling support for Struct, Array, and JSON string types with full predicate pushdown capabilities. The implementation leverages tantivy4java 0.25.2+ JSON field support to provide high-performance querying of complex nested data structures while maintaining backward compatibility with existing tables.

**Key Benefits:**
- ✅ Eliminates `UnsupportedOperationException` for Struct/Array types
- ✅ Enables powerful JSON querying with IndexQuery operators
- ✅ Maintains high performance with native predicate pushdown
- ✅ Preserves backward compatibility with existing functionality
- ✅ Provides flexible configuration for different use cases

**Next Steps:**
1. Review and approve design
2. Begin Phase 1 implementation (Core Infrastructure)
3. Iterative testing and refinement
4. Documentation and release preparation

---

**Document Version**: 1.0
**Last Updated**: 2025-10-29
**Authors**: Claude Code (Design)
**Reviewers**: [To be filled]
