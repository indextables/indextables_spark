# Bucket Aggregation Integration Design

## Overview

This document describes the design for integrating tantivy4java's bucket aggregations (DateHistogram, Histogram, Range) into IndexTables4Spark's aggregate pushdown framework. The design follows a hybrid approach: explicit SQL functions for full control, with a future path to automatic detection of standard Spark expressions.

## Goals

1. **Enable time-series analytics** - Allow users to efficiently aggregate by time intervals (hourly, daily, etc.)
2. **Support numeric distribution analysis** - Histogram bucketing for price ranges, scores, etc.
3. **Allow custom range bucketing** - Business-defined tiers (budget/mid/premium)
4. **Maintain Spark SQL compatibility** - Work with standard SQL interface and DataFrame API
5. **Expose full tantivy options** - minDocCount, offset, bounds, etc.
6. **Preserve existing functionality** - TermsAggregation for regular GROUP BY continues to work

## User-Facing Syntax

### Date Histogram

```sql
-- Basic usage: group by hour
SELECT indextables_date_histogram(timestamp, '1h') as hour_bucket,
       COUNT(*), SUM(amount), AVG(rating)
FROM events
GROUP BY 1

-- With offset (start buckets at 6am instead of midnight)
SELECT indextables_date_histogram(timestamp, '1d', offset => '6h') as day_bucket,
       COUNT(*)
FROM events
GROUP BY 1

-- With bounds (limit time range)
SELECT indextables_date_histogram(timestamp, '1h',
         hard_bounds_min => '2024-01-01T00:00:00Z',
         hard_bounds_max => '2024-01-31T23:59:59Z') as hour_bucket,
       COUNT(*)
FROM events
GROUP BY 1

-- Skip empty buckets
SELECT indextables_date_histogram(timestamp, '1h', min_doc_count => 1) as hour_bucket,
       COUNT(*)
FROM events
GROUP BY 1
```

**Supported intervals:** `1ms`, `10ms`, `100ms`, `1s`, `30s`, `1m`, `5m`, `15m`, `30m`, `1h`, `6h`, `12h`, `1d`, `7d`

### Numeric Histogram

```sql
-- Basic usage: $100 price buckets
SELECT indextables_histogram(price, 100) as price_bucket,
       COUNT(*), AVG(rating)
FROM products
GROUP BY 1

-- With offset (shift bucket boundaries)
SELECT indextables_histogram(score, 10, offset => 5) as score_bucket,
       COUNT(*)
FROM results
GROUP BY 1

-- With bounds
SELECT indextables_histogram(price, 50,
         hard_bounds_min => 0,
         hard_bounds_max => 500) as price_bucket,
       COUNT(*)
FROM products
GROUP BY 1

-- Extended bounds (force bucket creation even if empty)
SELECT indextables_histogram(rating, 1,
         extended_bounds_min => 1,
         extended_bounds_max => 5) as rating_bucket,
       COUNT(*)
FROM reviews
GROUP BY 1
```

### Range Aggregation

```sql
-- Named ranges with explicit bounds
SELECT indextables_range(price,
         'budget' => (NULL, 50),
         'mid' => (50, 200),
         'premium' => (200, NULL)) as tier,
       COUNT(*), AVG(rating)
FROM products
GROUP BY 1

-- Shorthand syntax
SELECT indextables_range(price, NULL:50, 50:200, 200:NULL) as tier,
       COUNT(*)
FROM products
GROUP BY 1

-- With custom names
SELECT indextables_range(age,
         'children' => (NULL, 13),
         'teenagers' => (13, 20),
         'adults' => (20, 65),
         'seniors' => (65, NULL)) as age_group,
       COUNT(*)
FROM users
GROUP BY 1
```

### DataFrame API (via expr)

```scala
import org.apache.spark.sql.functions._

// Date histogram
df.groupBy(expr("indextables_date_histogram(timestamp, '1h')").as("hour"))
  .agg(count("*"), sum("amount"))

// Numeric histogram
df.groupBy(expr("indextables_histogram(price, 100)").as("price_bucket"))
  .agg(count("*"), avg("rating"))

// Range
df.groupBy(expr("indextables_range(price, 'budget' => (NULL, 50), 'mid' => (50, 200), 'premium' => (200, NULL))").as("tier"))
  .agg(count("*"))
```

### PySpark

```python
from pyspark.sql.functions import expr

# Date histogram
df.groupBy(expr("indextables_date_histogram(timestamp, '1h')").alias("hour")) \
  .agg(count("*"), sum("amount"))

# Numeric histogram
df.groupBy(expr("indextables_histogram(price, 100)").alias("price_bucket")) \
  .agg(count("*"), avg("rating"))
```

## Architecture

### Component Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              SQL Parser Extension                            │
│  IndexTables4SparkSqlParser                                                  │
│  ├── indextables_date_histogram(col, interval, ...)                         │
│  ├── indextables_histogram(col, interval, ...)                              │
│  └── indextables_range(col, range1, range2, ...)                            │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Catalyst Expressions                               │
│  io.indextables.spark.expressions                                           │
│  ├── DateHistogramExpression(timeCol, interval, offset, minDocCount, ...)   │
│  ├── HistogramExpression(numericCol, interval, offset, minDocCount, ...)    │
│  └── RangeExpression(numericCol, ranges: Seq[RangeBucket])                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           ScanBuilder Integration                            │
│  IndexTables4SparkScanBuilder.pushAggregation()                             │
│  ├── Detect bucket expressions in groupByExpressions                        │
│  ├── Extract parameters (field, interval, options)                          │
│  ├── Validate field is fast field (for aggregation efficiency)              │
│  └── Store bucket config for scan creation                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         Bucket Aggregate Scan                                │
│  IndexTables4SparkBucketAggregateScan                                       │
│  ├── Creates appropriate tantivy4java aggregation                           │
│  │   ├── DateHistogramAggregation                                           │
│  │   ├── HistogramAggregation                                               │
│  │   └── RangeAggregation                                                   │
│  ├── Adds sub-aggregations (SUM, COUNT, MIN, MAX)                           │
│  └── Returns bucket results as InternalRow                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                         tantivy4java Execution                               │
│  SplitSearcher.search(query, 0, aggName, bucketAggregation)                 │
│  └── Returns DateHistogramResult / HistogramResult / RangeResult            │
└─────────────────────────────────────────────────────────────────────────────┘
```

### New Files

```
src/main/scala/io/indextables/spark/
├── expressions/
│   ├── BucketExpressions.scala          # DateHistogram, Histogram, Range expressions
│   └── BucketExpressionUtils.scala      # Parsing and validation utilities
├── core/
│   └── IndexTables4SparkBucketAggregateScan.scala  # New scan implementation
└── parser/
    └── BucketFunctionParser.scala       # SQL function parsing
```

### Modified Files

```
src/main/scala/io/indextables/spark/
├── core/
│   └── IndexTables4SparkScanBuilder.scala   # Detect bucket expressions
├── extensions/
│   └── IndexTables4SparkExtensions.scala    # Register new functions
└── parser/
    └── IndexTables4SparkSqlParser.scala     # Add function grammar
```

## Detailed Design

### 1. Catalyst Expressions

```scala
// src/main/scala/io/indextables/spark/expressions/BucketExpressions.scala

package io.indextables.spark.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._

/**
 * Base trait for bucket aggregation expressions.
 * These expressions are Unevaluable - they exist only to carry configuration
 * to the aggregate pushdown layer.
 */
sealed trait BucketExpression extends Expression with Unevaluable {
  def field: Expression
  def fieldName: String = field match {
    case a: Attribute => a.name
    case _ => throw new AnalysisException(s"Bucket expression requires a column reference, got: $field")
  }
}

/**
 * Date histogram bucket expression.
 *
 * @param field The timestamp/date column to bucket
 * @param interval The bucket interval (e.g., "1h", "1d", "30m")
 * @param offset Optional offset to shift bucket boundaries (e.g., "6h")
 * @param minDocCount Minimum document count for bucket to be returned (default: 0)
 * @param hardBoundsMin Optional minimum timestamp bound
 * @param hardBoundsMax Optional maximum timestamp bound
 * @param extendedBoundsMin Optional extended bounds minimum (force bucket creation)
 * @param extendedBoundsMax Optional extended bounds maximum (force bucket creation)
 */
case class DateHistogramExpression(
    field: Expression,
    interval: String,
    offset: Option[String] = None,
    minDocCount: Long = 0,
    hardBoundsMin: Option[Long] = None,   // Epoch microseconds
    hardBoundsMax: Option[Long] = None,
    extendedBoundsMin: Option[Long] = None,
    extendedBoundsMax: Option[Long] = None
) extends BucketExpression {

  override def dataType: DataType = StructType(Seq(
    StructField("bucket_start", TimestampType, nullable = false),
    StructField("bucket_end", TimestampType, nullable = false)
  ))

  override def nullable: Boolean = false
  override def children: Seq[Expression] = Seq(field)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)

  // Validate interval format
  require(isValidInterval(interval),
    s"Invalid date histogram interval: $interval. " +
    s"Supported: 1ms, 10ms, 100ms, 1s, 30s, 1m, 5m, 15m, 30m, 1h, 6h, 12h, 1d, 7d")

  private def isValidInterval(i: String): Boolean = {
    val pattern = """(\d+)(ms|s|m|h|d)""".r
    i match {
      case pattern(_, _) => true
      case _ => false
    }
  }
}

/**
 * Numeric histogram bucket expression.
 *
 * @param field The numeric column to bucket
 * @param interval The fixed bucket width
 * @param offset Optional offset to shift bucket boundaries
 * @param minDocCount Minimum document count for bucket to be returned
 * @param hardBoundsMin Optional minimum bound
 * @param hardBoundsMax Optional maximum bound
 * @param extendedBoundsMin Optional extended bounds minimum
 * @param extendedBoundsMax Optional extended bounds maximum
 */
case class HistogramExpression(
    field: Expression,
    interval: Double,
    offset: Double = 0.0,
    minDocCount: Long = 0,
    hardBoundsMin: Option[Double] = None,
    hardBoundsMax: Option[Double] = None,
    extendedBoundsMin: Option[Double] = None,
    extendedBoundsMax: Option[Double] = None
) extends BucketExpression {

  require(interval > 0, s"Histogram interval must be positive, got: $interval")

  override def dataType: DataType = DoubleType  // Returns bucket lower bound
  override def nullable: Boolean = false
  override def children: Seq[Expression] = Seq(field)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)
}

/**
 * Range bucket expression with custom named ranges.
 *
 * @param field The numeric column to bucket
 * @param ranges Sequence of named ranges with (name, from, to) where from/to can be None for unbounded
 */
case class RangeExpression(
    field: Expression,
    ranges: Seq[RangeBucket]
) extends BucketExpression {

  require(ranges.nonEmpty, "Range expression requires at least one range")

  // Validate ranges don't overlap (tantivy requirement)
  validateNoOverlap()

  override def dataType: DataType = StringType  // Returns range key/name
  override def nullable: Boolean = false
  override def children: Seq[Expression] = Seq(field)

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): Expression =
    copy(field = newChildren.head)

  private def validateNoOverlap(): Unit = {
    val sortedRanges = ranges.sortBy(_.from.getOrElse(Double.MinValue))
    for (i <- 0 until sortedRanges.length - 1) {
      val current = sortedRanges(i)
      val next = sortedRanges(i + 1)
      current.to.foreach { currentTo =>
        next.from.foreach { nextFrom =>
          require(currentTo <= nextFrom,
            s"Overlapping ranges not supported: ${current.key} ends at $currentTo " +
            s"but ${next.key} starts at $nextFrom")
        }
      }
    }
  }
}

/**
 * A single range bucket definition.
 *
 * @param key The name/key for this bucket
 * @param from Lower bound (inclusive), None for unbounded
 * @param to Upper bound (exclusive), None for unbounded
 */
case class RangeBucket(
    key: String,
    from: Option[Double],
    to: Option[Double]
) {
  require(key.nonEmpty, "Range bucket key cannot be empty")
  require(from.isDefined || to.isDefined,
    s"Range bucket '$key' must have at least one bound defined")
}
```

### 2. SQL Parser Extension

```scala
// src/main/scala/io/indextables/spark/parser/BucketFunctionParser.scala

package io.indextables.spark.parser

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.ParserInterface
import io.indextables.spark.expressions._

/**
 * Parser for bucket aggregation SQL functions.
 */
object BucketFunctionParser {

  /**
   * Parse indextables_date_histogram function call.
   *
   * Syntax:
   *   indextables_date_histogram(column, interval)
   *   indextables_date_histogram(column, interval, offset => 'value')
   *   indextables_date_histogram(column, interval, min_doc_count => N)
   *   indextables_date_histogram(column, interval,
   *     offset => 'value',
   *     min_doc_count => N,
   *     hard_bounds_min => 'timestamp',
   *     hard_bounds_max => 'timestamp')
   */
  def parseDateHistogram(
      args: Seq[Expression],
      namedArgs: Map[String, Expression]
  ): DateHistogramExpression = {
    require(args.length >= 2,
      "indextables_date_histogram requires at least 2 arguments: (column, interval)")

    val field = args(0)
    val interval = extractStringLiteral(args(1), "interval")

    val offset = namedArgs.get("offset").map(e => extractStringLiteral(e, "offset"))
    val minDocCount = namedArgs.get("min_doc_count")
      .map(e => extractLongLiteral(e, "min_doc_count"))
      .getOrElse(0L)
    val hardBoundsMin = namedArgs.get("hard_bounds_min")
      .map(e => extractTimestampLiteral(e, "hard_bounds_min"))
    val hardBoundsMax = namedArgs.get("hard_bounds_max")
      .map(e => extractTimestampLiteral(e, "hard_bounds_max"))
    val extendedBoundsMin = namedArgs.get("extended_bounds_min")
      .map(e => extractTimestampLiteral(e, "extended_bounds_min"))
    val extendedBoundsMax = namedArgs.get("extended_bounds_max")
      .map(e => extractTimestampLiteral(e, "extended_bounds_max"))

    DateHistogramExpression(
      field = field,
      interval = interval,
      offset = offset,
      minDocCount = minDocCount,
      hardBoundsMin = hardBoundsMin,
      hardBoundsMax = hardBoundsMax,
      extendedBoundsMin = extendedBoundsMin,
      extendedBoundsMax = extendedBoundsMax
    )
  }

  /**
   * Parse indextables_histogram function call.
   *
   * Syntax:
   *   indextables_histogram(column, interval)
   *   indextables_histogram(column, interval, offset => N)
   *   indextables_histogram(column, interval, min_doc_count => N)
   */
  def parseHistogram(
      args: Seq[Expression],
      namedArgs: Map[String, Expression]
  ): HistogramExpression = {
    require(args.length >= 2,
      "indextables_histogram requires at least 2 arguments: (column, interval)")

    val field = args(0)
    val interval = extractDoubleLiteral(args(1), "interval")

    val offset = namedArgs.get("offset")
      .map(e => extractDoubleLiteral(e, "offset"))
      .getOrElse(0.0)
    val minDocCount = namedArgs.get("min_doc_count")
      .map(e => extractLongLiteral(e, "min_doc_count"))
      .getOrElse(0L)
    val hardBoundsMin = namedArgs.get("hard_bounds_min")
      .map(e => extractDoubleLiteral(e, "hard_bounds_min"))
    val hardBoundsMax = namedArgs.get("hard_bounds_max")
      .map(e => extractDoubleLiteral(e, "hard_bounds_max"))
    val extendedBoundsMin = namedArgs.get("extended_bounds_min")
      .map(e => extractDoubleLiteral(e, "extended_bounds_min"))
    val extendedBoundsMax = namedArgs.get("extended_bounds_max")
      .map(e => extractDoubleLiteral(e, "extended_bounds_max"))

    HistogramExpression(
      field = field,
      interval = interval,
      offset = offset,
      minDocCount = minDocCount,
      hardBoundsMin = hardBoundsMin,
      hardBoundsMax = hardBoundsMax,
      extendedBoundsMin = extendedBoundsMin,
      extendedBoundsMax = extendedBoundsMax
    )
  }

  /**
   * Parse indextables_range function call.
   *
   * Syntax:
   *   indextables_range(column, 'name1' => (from1, to1), 'name2' => (from2, to2), ...)
   *   indextables_range(column, from1:to1, from2:to2, ...)  -- auto-named
   */
  def parseRange(
      args: Seq[Expression],
      namedArgs: Map[String, Expression]
  ): RangeExpression = {
    require(args.nonEmpty, "indextables_range requires at least column argument")

    val field = args(0)

    // Parse named ranges from namedArgs
    val ranges = namedArgs.map { case (name, expr) =>
      parseRangeBucket(name, expr)
    }.toSeq

    require(ranges.nonEmpty,
      "indextables_range requires at least one range definition")

    RangeExpression(field = field, ranges = ranges)
  }

  private def parseRangeBucket(name: String, expr: Expression): RangeBucket = {
    // Expected format: struct(from, to) or array(from, to)
    // Handle NULL for unbounded
    expr match {
      case struct if struct.dataType.isInstanceOf[org.apache.spark.sql.types.StructType] =>
        val children = struct.children
        require(children.length == 2,
          s"Range '$name' must have exactly 2 values (from, to)")
        val from = extractOptionalDouble(children(0))
        val to = extractOptionalDouble(children(1))
        RangeBucket(name, from, to)
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid range format for '$name'. Expected (from, to) tuple.")
    }
  }

  // Helper methods for literal extraction
  private def extractStringLiteral(expr: Expression, paramName: String): String = {
    expr match {
      case lit: org.apache.spark.sql.catalyst.expressions.Literal =>
        lit.value.toString
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a string literal, got: $expr")
    }
  }

  private def extractDoubleLiteral(expr: Expression, paramName: String): Double = {
    expr match {
      case lit: org.apache.spark.sql.catalyst.expressions.Literal =>
        lit.value match {
          case n: Number => n.doubleValue()
          case s: String => s.toDouble
          case _ => throw new IllegalArgumentException(
            s"$paramName must be a number, got: ${lit.value}")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a numeric literal, got: $expr")
    }
  }

  private def extractLongLiteral(expr: Expression, paramName: String): Long = {
    expr match {
      case lit: org.apache.spark.sql.catalyst.expressions.Literal =>
        lit.value match {
          case n: Number => n.longValue()
          case _ => throw new IllegalArgumentException(
            s"$paramName must be a number, got: ${lit.value}")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a numeric literal, got: $expr")
    }
  }

  private def extractTimestampLiteral(expr: Expression, paramName: String): Long = {
    expr match {
      case lit: org.apache.spark.sql.catalyst.expressions.Literal =>
        lit.value match {
          case n: Number => n.longValue()
          case s: String =>
            java.time.Instant.parse(s).toEpochMilli * 1000 // to micros
          case _ => throw new IllegalArgumentException(
            s"$paramName must be a timestamp, got: ${lit.value}")
        }
      case _ =>
        throw new IllegalArgumentException(
          s"$paramName must be a timestamp literal, got: $expr")
    }
  }

  private def extractOptionalDouble(expr: Expression): Option[Double] = {
    expr match {
      case lit: org.apache.spark.sql.catalyst.expressions.Literal if lit.value == null =>
        None
      case lit: org.apache.spark.sql.catalyst.expressions.Literal =>
        lit.value match {
          case n: Number => Some(n.doubleValue())
          case _ => None
        }
      case _ => None
    }
  }
}
```

### 3. ScanBuilder Integration

```scala
// Additions to IndexTables4SparkScanBuilder.scala

// New state for bucket aggregations
private var _bucketConfig: Option[BucketAggregationConfig] = None

/**
 * Configuration for bucket aggregations extracted from GROUP BY expressions.
 */
sealed trait BucketAggregationConfig {
  def fieldName: String
}

case class DateHistogramConfig(
    fieldName: String,
    interval: String,
    offset: Option[String],
    minDocCount: Long,
    hardBoundsMin: Option[Long],
    hardBoundsMax: Option[Long],
    extendedBoundsMin: Option[Long],
    extendedBoundsMax: Option[Long]
) extends BucketAggregationConfig

case class HistogramConfig(
    fieldName: String,
    interval: Double,
    offset: Double,
    minDocCount: Long,
    hardBoundsMin: Option[Double],
    hardBoundsMax: Option[Double],
    extendedBoundsMin: Option[Double],
    extendedBoundsMax: Option[Double]
) extends BucketAggregationConfig

case class RangeConfig(
    fieldName: String,
    ranges: Seq[RangeBucket]
) extends BucketAggregationConfig

override def pushAggregation(aggregation: Aggregation): Boolean = {
  logger.debug(s"AGGREGATE PUSHDOWN: Received aggregation request: $aggregation")

  val groupByExpressions = aggregation.groupByExpressions()
  val hasGroupBy = groupByExpressions != null && groupByExpressions.nonEmpty

  if (hasGroupBy) {
    // Check for bucket expressions FIRST
    val bucketConfig = detectBucketExpression(groupByExpressions)

    bucketConfig match {
      case Some(config) =>
        logger.info(s"BUCKET AGGREGATION: Detected ${config.getClass.getSimpleName} on field '${config.fieldName}'")

        // Validate field is a fast field for efficient aggregation
        if (!isFieldFastField(config.fieldName)) {
          logger.warn(s"BUCKET AGGREGATION: Field '${config.fieldName}' is not a fast field. " +
            s"Configure with spark.indextables.indexing.fastfields to improve performance.")
        }

        // Store bucket config
        _bucketConfig = Some(config)

        // Validate aggregations are compatible
        if (!areAggregationsCompatibleWithBucket(aggregation)) {
          logger.debug(s"BUCKET AGGREGATION: Aggregations not compatible")
          return false
        }

        _pushedAggregation = Some(aggregation)
        logger.info(s"BUCKET AGGREGATION: Accepted - will use ${config.getClass.getSimpleName}")
        return true

      case None =>
        // Fall through to regular GROUP BY handling
        logger.debug(s"BUCKET AGGREGATION: No bucket expression detected, using TermsAggregation")
    }

    // ... existing GROUP BY logic with TermsAggregation ...
  }

  // ... existing simple aggregation logic ...
}

/**
 * Detect bucket aggregation expressions in GROUP BY clauses.
 */
private def detectBucketExpression(
    groupByExpressions: Array[org.apache.spark.sql.connector.expressions.Expression]
): Option[BucketAggregationConfig] = {

  // We only support single bucket expression in GROUP BY
  if (groupByExpressions.length != 1) {
    return None
  }

  val expr = groupByExpressions(0)

  // Check for our custom bucket expressions by examining the expression tree
  // V2 expressions wrap Catalyst expressions, so we need to extract them
  extractBucketConfig(expr)
}

private def extractBucketConfig(
    expr: org.apache.spark.sql.connector.expressions.Expression
): Option[BucketAggregationConfig] = {

  // The V2 expression wraps our Catalyst expressions
  // Check the expression class name and extract configuration
  val exprStr = expr.toString
  val exprClass = expr.getClass.getSimpleName

  logger.debug(s"BUCKET DETECTION: Checking expression: $exprStr (class: $exprClass)")

  // Pattern match on function name in string representation
  if (exprStr.contains("indextables_date_histogram")) {
    extractDateHistogramConfig(expr)
  } else if (exprStr.contains("indextables_histogram")) {
    extractHistogramConfig(expr)
  } else if (exprStr.contains("indextables_range")) {
    extractRangeConfig(expr)
  } else {
    None
  }
}

// ... extraction helper methods ...

/**
 * Check if aggregations are compatible with bucket aggregations.
 * Supports: COUNT, COUNT(*), SUM, AVG (as SUM+COUNT), MIN, MAX
 */
private def areAggregationsCompatibleWithBucket(aggregation: Aggregation): Boolean = {
  import org.apache.spark.sql.connector.expressions.aggregate._

  aggregation.aggregateExpressions.forall {
    case _: Count | _: CountStar | _: Sum | _: Avg | _: Min | _: Max => true
    case other =>
      logger.debug(s"BUCKET AGGREGATION: Unsupported aggregation type: ${other.getClass.getSimpleName}")
      false
  }
}

// In createAggregateScan, add bucket handling:
private def createAggregateScan(aggregation: Aggregation, effectiveFilters: Array[Filter]): Scan = {
  _bucketConfig match {
    case Some(config) =>
      // Create bucket aggregate scan
      logger.info(s"AGGREGATE SCAN: Creating bucket aggregation scan for ${config.getClass.getSimpleName}")
      createBucketAggregateScan(aggregation, config, effectiveFilters)

    case None =>
      // Existing logic for GROUP BY or simple aggregation
      _pushedGroupBy match {
        case Some(groupByColumns) => createGroupByAggregateScan(aggregation, groupByColumns, effectiveFilters)
        case None =>
          if (canUseTransactionLogCount(aggregation, effectiveFilters)) {
            createTransactionLogCountScan(aggregation, effectiveFilters)
          } else {
            createSimpleAggregateScan(aggregation, effectiveFilters)
          }
      }
  }
}

private def createBucketAggregateScan(
    aggregation: Aggregation,
    bucketConfig: BucketAggregationConfig,
    effectiveFilters: Array[Filter]
): Scan = {
  val extractedIndexQueryFilters = extractIndexQueriesFromCurrentPlan()

  new IndexTables4SparkBucketAggregateScan(
    sparkSession,
    transactionLog,
    schema,
    effectiveFilters,
    options,
    config,
    aggregation,
    bucketConfig,
    extractedIndexQueryFilters
  )
}
```

### 4. Bucket Aggregate Scan Implementation

```scala
// src/main/scala/io/indextables/spark/core/IndexTables4SparkBucketAggregateScan.scala

package io.indextables.spark.core

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate.Aggregation
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.SparkSession
import org.apache.spark.unsafe.types.UTF8String

import io.indextables.spark.transaction.TransactionLog
import io.indextables.tantivy4java.aggregation._
import io.indextables.tantivy4java.split.{SplitCacheManager, SplitMatchAllQuery}
import org.slf4j.LoggerFactory

/**
 * Specialized scan for bucket aggregation operations (DateHistogram, Histogram, Range).
 * Executes tantivy bucket aggregations and returns results as Spark rows.
 */
class IndexTables4SparkBucketAggregateScan(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    schema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    config: Map[String, String],
    aggregation: Aggregation,
    bucketConfig: BucketAggregationConfig,
    indexQueryFilters: Array[Any] = Array.empty
) extends Scan {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkBucketAggregateScan])

  logger.info(s"BUCKET AGGREGATE SCAN: Created for ${bucketConfig.getClass.getSimpleName} on field '${bucketConfig.fieldName}'")

  override def readSchema(): StructType = {
    createBucketResultSchema(aggregation, bucketConfig)
  }

  override def toBatch: Batch = {
    new IndexTables4SparkBucketAggregateBatch(
      sparkSession,
      transactionLog,
      schema,
      pushedFilters,
      options,
      config,
      aggregation,
      bucketConfig,
      indexQueryFilters
    )
  }

  override def description(): String = {
    val aggDesc = aggregation.aggregateExpressions.map(_.toString).mkString(", ")
    s"IndexTables4SparkBucketAggregateScan[bucket=${bucketConfig.getClass.getSimpleName}, field=${bucketConfig.fieldName}, aggregations=[$aggDesc]]"
  }

  /**
   * Create schema for bucket aggregation results.
   * Schema follows Spark's expected naming: group_col_0, agg_func_0, agg_func_1, ...
   */
  private def createBucketResultSchema(
      aggregation: Aggregation,
      bucketConfig: BucketAggregationConfig
  ): StructType = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    // Bucket column type depends on bucket type
    val bucketField = bucketConfig match {
      case _: DateHistogramConfig =>
        // Date histogram returns timestamp of bucket start
        StructField("group_col_0", TimestampType, nullable = false)
      case _: HistogramConfig =>
        // Numeric histogram returns bucket lower bound
        StructField("group_col_0", DoubleType, nullable = false)
      case _: RangeConfig =>
        // Range returns the range key/name
        StructField("group_col_0", StringType, nullable = false)
    }

    // Aggregation result columns
    val aggregationFields = aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        val dataType = aggExpr match {
          case _: Count | _: CountStar => LongType
          case sum: Sum => getAggregateOutputType(sum, schema)
          case _: Avg => DoubleType
          case min: Min => getInputFieldType(min, schema)
          case max: Max => getInputFieldType(max, schema)
          case _ => LongType
        }
        StructField(s"agg_func_$index", dataType, nullable = true)
    }

    StructType(Seq(bucketField) ++ aggregationFields)
  }

  private def getAggregateOutputType(
      aggExpr: org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc,
      schema: StructType
  ): DataType = {
    val fieldType = getInputFieldType(aggExpr, schema)
    fieldType match {
      case IntegerType | LongType => LongType
      case FloatType | DoubleType => DoubleType
      case _ => DoubleType
    }
  }

  private def getInputFieldType(
      aggExpr: org.apache.spark.sql.connector.expressions.aggregate.AggregateFunc,
      schema: StructType
  ): DataType = {
    val column = aggExpr.children().headOption.getOrElse(return LongType)
    val fieldName = io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    schema.fields.find(_.name == fieldName).map(_.dataType).getOrElse(LongType)
  }
}

/**
 * Batch implementation for bucket aggregations.
 */
class IndexTables4SparkBucketAggregateBatch(
    sparkSession: SparkSession,
    transactionLog: TransactionLog,
    schema: StructType,
    pushedFilters: Array[Filter],
    options: CaseInsensitiveStringMap,
    config: Map[String, String],
    aggregation: Aggregation,
    bucketConfig: BucketAggregationConfig,
    indexQueryFilters: Array[Any] = Array.empty
) extends Batch {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkBucketAggregateBatch])

  override def planInputPartitions(): Array[InputPartition] = {
    val sparkContext = sparkSession.sparkContext
    val availableHosts = io.indextables.spark.storage.DriverSplitLocalityManager.getAvailableHosts(sparkContext)

    val allSplits = transactionLog.listFiles()

    // Apply data skipping
    val helperScan = new IndexTables4SparkScan(
      sparkSession, transactionLog, schema, pushedFilters, options, None, config, indexQueryFilters
    )
    val filteredSplits = helperScan.applyDataSkipping(allSplits, pushedFilters)

    logger.debug(s"BUCKET BATCH: Planning ${filteredSplits.length} partitions after data skipping")

    // Assign splits to hosts
    val splitPaths = filteredSplits.map(_.path)
    val assignments = io.indextables.spark.storage.DriverSplitLocalityManager.assignSplitsForQuery(splitPaths, availableHosts)

    filteredSplits.map { split =>
      new IndexTables4SparkBucketAggregatePartition(
        split,
        pushedFilters,
        config,
        aggregation,
        bucketConfig,
        transactionLog.getTablePath(),
        schema,
        indexQueryFilters,
        assignments.get(split.path)
      )
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = {
    new IndexTables4SparkBucketAggregateReaderFactory(
      sparkSession, pushedFilters, config, aggregation, bucketConfig, schema, indexQueryFilters
    )
  }
}

/**
 * Input partition for bucket aggregation processing.
 */
class IndexTables4SparkBucketAggregatePartition(
    val split: io.indextables.spark.transaction.AddAction,
    val pushedFilters: Array[Filter],
    val config: Map[String, String],
    val aggregation: Aggregation,
    val bucketConfig: BucketAggregationConfig,
    val tablePath: org.apache.hadoop.fs.Path,
    val schema: StructType,
    val indexQueryFilters: Array[Any] = Array.empty,
    val preferredHost: Option[String] = None
) extends InputPartition {
  override def preferredLocations(): Array[String] = preferredHost.toArray
}

/**
 * Reader factory for bucket aggregation partitions.
 */
class IndexTables4SparkBucketAggregateReaderFactory(
    sparkSession: SparkSession,
    pushedFilters: Array[Filter],
    config: Map[String, String],
    aggregation: Aggregation,
    bucketConfig: BucketAggregationConfig,
    schema: StructType,
    indexQueryFilters: Array[Any] = Array.empty
) extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    partition match {
      case p: IndexTables4SparkBucketAggregatePartition =>
        new IndexTables4SparkBucketAggregateReader(p, sparkSession, schema)
      case other =>
        throw new IllegalArgumentException(s"Unexpected partition type: ${other.getClass}")
    }
  }
}

/**
 * Reader for bucket aggregation partitions.
 * Executes tantivy bucket aggregations and converts results to Spark rows.
 */
class IndexTables4SparkBucketAggregateReader(
    partition: IndexTables4SparkBucketAggregatePartition,
    sparkSession: SparkSession,
    schema: StructType
) extends PartitionReader[InternalRow] {

  private val logger = LoggerFactory.getLogger(classOf[IndexTables4SparkBucketAggregateReader])
  private var bucketResults: Iterator[InternalRow] = _
  private var isInitialized = false

  override def next(): Boolean = {
    if (!isInitialized) {
      initialize()
      isInitialized = true
    }
    bucketResults.hasNext
  }

  override def get(): InternalRow = bucketResults.next()

  override def close(): Unit = {
    val bytesRead = partition.split.size
    org.apache.spark.sql.indextables.OutputMetricsUpdater.incInputMetrics(bytesRead, 0)
  }

  private def initialize(): Unit = {
    logger.debug(s"BUCKET READER: Initializing for split: ${partition.split.path}")

    try {
      val results = executeBucketAggregation()
      bucketResults = results.iterator
      logger.debug(s"BUCKET READER: Completed with ${results.length} bucket rows")
    } catch {
      case e: Exception =>
        logger.warn(s"BUCKET READER: Failed to execute bucket aggregation", e)
        bucketResults = Iterator.empty
    }
  }

  /**
   * Execute the bucket aggregation using tantivy4java.
   */
  private def executeBucketAggregation(): Array[InternalRow] = {
    val splitCacheConfig = createCacheConfig()
    val cacheManager = SplitCacheManager.getInstance(splitCacheConfig.toJavaCacheConfig())

    val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
      partition.split.path,
      partition.tablePath.toString
    ).replace("s3a://", "s3://")

    val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
      partition.split,
      partition.tablePath.toString
    )

    val searcher = cacheManager.createSplitSearcher(resolvedPath, splitMetadata)

    // Create the bucket aggregation based on config type
    val (aggName, bucketAgg) = createBucketAggregation()

    // Add sub-aggregations for each metric
    addSubAggregations(bucketAgg)

    // Build query from filters
    val query = buildQuery()

    logger.debug(s"BUCKET EXECUTION: Running ${partition.bucketConfig.getClass.getSimpleName} aggregation")

    val result = searcher.search(query, 0, aggName, bucketAgg)

    if (result.hasAggregations()) {
      val aggResult = result.getAggregation(aggName)
      if (aggResult != null) {
        convertBucketResultsToRows(aggResult)
      } else {
        Array.empty[InternalRow]
      }
    } else {
      Array.empty[InternalRow]
    }
  }

  /**
   * Create the appropriate tantivy bucket aggregation.
   */
  private def createBucketAggregation(): (String, SplitAggregation) = {
    partition.bucketConfig match {
      case config: DateHistogramConfig =>
        val agg = new DateHistogramAggregation("bucket_agg", config.fieldName, config.interval)
        config.offset.foreach(o => agg.setOffset(o))
        if (config.minDocCount > 0) agg.setMinDocCount(config.minDocCount)
        config.hardBoundsMin.foreach(min =>
          config.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        config.extendedBoundsMin.foreach(min =>
          config.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        ("bucket_agg", agg)

      case config: HistogramConfig =>
        val agg = new HistogramAggregation("bucket_agg", config.fieldName, config.interval)
        if (config.offset != 0.0) agg.setOffset(config.offset)
        if (config.minDocCount > 0) agg.setMinDocCount(config.minDocCount)
        config.hardBoundsMin.foreach(min =>
          config.hardBoundsMax.foreach(max => agg.setHardBounds(min, max)))
        config.extendedBoundsMin.foreach(min =>
          config.extendedBoundsMax.foreach(max => agg.setExtendedBounds(min, max)))
        ("bucket_agg", agg)

      case config: RangeConfig =>
        val agg = new RangeAggregation("bucket_agg", config.fieldName)
        config.ranges.foreach { range =>
          agg.addRange(
            range.key,
            range.from.map(java.lang.Double.valueOf).orNull,
            range.to.map(java.lang.Double.valueOf).orNull
          )
        }
        ("bucket_agg", agg)
    }
  }

  /**
   * Add sub-aggregations for metrics (SUM, MIN, MAX, etc.)
   */
  private def addSubAggregations(bucketAgg: SplitAggregation): Unit = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.foreach {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            // COUNT uses bucket doc count, no sub-aggregation needed

          case sum: Sum =>
            val fieldName = getFieldName(sum.column)
            addSubAgg(bucketAgg, s"sum_$index", new SumAggregation(fieldName))

          case min: Min =>
            val fieldName = getFieldName(min.column)
            addSubAgg(bucketAgg, s"min_$index", new MinAggregation(fieldName))

          case max: Max =>
            val fieldName = getFieldName(max.column)
            addSubAgg(bucketAgg, s"max_$index", new MaxAggregation(fieldName))

          case _: Avg =>
            // AVG is transformed to SUM + COUNT by Spark when supportCompletePushDown=false
            throw new IllegalStateException("AVG should have been transformed to SUM + COUNT")

          case other =>
            logger.warn(s"Unsupported aggregation in bucket: ${other.getClass.getSimpleName}")
        }
    }
  }

  private def addSubAgg(bucketAgg: SplitAggregation, name: String, subAgg: SplitAggregation): Unit = {
    bucketAgg match {
      case dh: DateHistogramAggregation => dh.addSubAggregation(subAgg)
      case h: HistogramAggregation => h.addSubAggregation(subAgg)
      case r: RangeAggregation =>
        // RangeAggregation sub-aggregations not exposed in current API
        logger.warn("Sub-aggregations on RangeAggregation not yet supported in tantivy4java")
    }
  }

  /**
   * Convert tantivy bucket results to Spark InternalRows.
   */
  private def convertBucketResultsToRows(aggResult: AggregationResult): Array[InternalRow] = {
    partition.bucketConfig match {
      case _: DateHistogramConfig =>
        val dateHistResult = aggResult.asInstanceOf[DateHistogramResult]
        dateHistResult.getBuckets.asScala.map { bucket =>
          val bucketKey = bucket.getKey  // epoch microseconds
          val aggValues = extractAggregationValues(bucket)
          InternalRow.fromSeq(Seq(bucketKey) ++ aggValues)
        }.toArray

      case _: HistogramConfig =>
        val histResult = aggResult.asInstanceOf[HistogramResult]
        histResult.getBuckets.asScala.map { bucket =>
          val bucketKey = bucket.getKey  // bucket lower bound
          val aggValues = extractAggregationValues(bucket)
          InternalRow.fromSeq(Seq(bucketKey) ++ aggValues)
        }.toArray

      case _: RangeConfig =>
        val rangeResult = aggResult.asInstanceOf[RangeResult]
        rangeResult.getBuckets.asScala.map { bucket =>
          val bucketKey = UTF8String.fromString(bucket.getKey)
          val aggValues = extractAggregationValuesFromRange(bucket)
          InternalRow.fromSeq(Seq(bucketKey) ++ aggValues)
        }.toArray
    }
  }

  /**
   * Extract aggregation values from a histogram/date histogram bucket.
   */
  private def extractAggregationValues(bucket: Any): Seq[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar =>
            bucket match {
              case dh: DateHistogramResult.DateHistogramBucket => dh.getDocCount.toLong
              case h: HistogramResult.HistogramBucket => h.getDocCount.toLong
            }

          case sum: Sum =>
            val sumResult = getSubAggregation(bucket, s"sum_$index").asInstanceOf[SumResult]
            if (sumResult != null) sumResult.getSum else 0.0

          case min: Min =>
            val minResult = getSubAggregation(bucket, s"min_$index").asInstanceOf[MinResult]
            if (minResult != null) minResult.getMin else null

          case max: Max =>
            val maxResult = getSubAggregation(bucket, s"max_$index").asInstanceOf[MaxResult]
            if (maxResult != null) maxResult.getMax else null

          case _ => null
        }
    }
  }

  private def extractAggregationValuesFromRange(bucket: RangeResult.RangeBucket): Seq[Any] = {
    import org.apache.spark.sql.connector.expressions.aggregate._

    partition.aggregation.aggregateExpressions.zipWithIndex.map {
      case (aggExpr, index) =>
        aggExpr match {
          case _: Count | _: CountStar => bucket.getDocCount.toLong
          case _ =>
            // Sub-aggregations on RangeAggregation not yet supported
            logger.warn(s"Sub-aggregation ${aggExpr.getClass.getSimpleName} on Range not supported")
            null
        }
    }
  }

  private def getSubAggregation(bucket: Any, name: String): AggregationResult = {
    bucket match {
      case dh: DateHistogramResult.DateHistogramBucket => dh.getSubAggregation(name)
      case h: HistogramResult.HistogramBucket => h.getSubAggregation(name, classOf[AggregationResult])
    }
  }

  private def buildQuery(): io.indextables.tantivy4java.split.SplitQuery = {
    val allFilters = partition.pushedFilters ++ partition.indexQueryFilters
    if (allFilters.nonEmpty) {
      val splitSearchEngine = createSplitSearchEngine()
      FiltersToQueryConverter.convertToSplitQuery(allFilters, splitSearchEngine, None, None)
    } else {
      new SplitMatchAllQuery()
    }
  }

  private def createCacheConfig(): io.indextables.spark.storage.SplitCacheConfig = {
    io.indextables.spark.util.ConfigUtils.createSplitCacheConfig(
      partition.config,
      Some(partition.tablePath.toString)
    )
  }

  private def createSplitSearchEngine(): io.indextables.spark.search.SplitSearchEngine = {
    val splitCacheConfig = createCacheConfig()
    val resolvedPath = PathResolutionUtils.resolveSplitPathAsString(
      partition.split.path,
      partition.tablePath.toString
    ).replace("s3a://", "s3://")

    val splitMetadata = io.indextables.spark.util.SplitMetadataFactory.fromAddAction(
      partition.split,
      partition.tablePath.toString
    )

    io.indextables.spark.search.SplitSearchEngine.fromSplitFileWithMetadata(
      partition.schema,
      resolvedPath,
      splitMetadata,
      splitCacheConfig,
      Some(IndexTables4SparkOptions(partition.config))
    )
  }

  private def getFieldName(column: org.apache.spark.sql.connector.expressions.Expression): String = {
    if (column.getClass.getSimpleName == "FieldReference") {
      column.toString
    } else {
      io.indextables.spark.util.ExpressionUtils.extractFieldName(column)
    }
  }
}
```

### 5. Function Registration

```scala
// Additions to IndexTables4SparkExtensions.scala

import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.analysis.FunctionRegistry
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}

class IndexTables4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Existing extensions...

    // Register bucket aggregation functions
    extensions.injectFunction(dateHistogramFunctionDesc)
    extensions.injectFunction(histogramFunctionDesc)
    extensions.injectFunction(rangeFunctionDesc)
  }

  private val dateHistogramFunctionDesc: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val funcId = FunctionIdentifier("indextables_date_histogram")
    val info = new ExpressionInfo(
      classOf[DateHistogramExpression].getName,
      "indextables_date_histogram",
      """
        |indextables_date_histogram(column, interval [, options]) - Groups data into time-based buckets.
        |
        |Arguments:
        |  column - A timestamp or date column
        |  interval - Bucket interval (e.g., '1h', '1d', '30m')
        |  offset => 'value' - Optional offset to shift bucket boundaries
        |  min_doc_count => N - Optional minimum document count (default: 0)
        |  hard_bounds_min => 'timestamp' - Optional minimum bound
        |  hard_bounds_max => 'timestamp' - Optional maximum bound
        |
        |Examples:
        |  SELECT indextables_date_histogram(ts, '1h') as hour, COUNT(*) FROM t GROUP BY 1
        |  SELECT indextables_date_histogram(ts, '1d', offset => '6h') as day, SUM(amt) FROM t GROUP BY 1
      """.stripMargin
    )
    val builder: Seq[Expression] => Expression = { args =>
      BucketFunctionParser.parseDateHistogram(args, Map.empty)
    }
    (funcId, info, builder)
  }

  private val histogramFunctionDesc: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val funcId = FunctionIdentifier("indextables_histogram")
    val info = new ExpressionInfo(
      classOf[HistogramExpression].getName,
      "indextables_histogram",
      """
        |indextables_histogram(column, interval [, options]) - Groups data into fixed-width numeric buckets.
        |
        |Arguments:
        |  column - A numeric column
        |  interval - Bucket width
        |  offset => N - Optional offset to shift bucket boundaries
        |  min_doc_count => N - Optional minimum document count (default: 0)
        |  hard_bounds_min => N - Optional minimum bound
        |  hard_bounds_max => N - Optional maximum bound
        |
        |Examples:
        |  SELECT indextables_histogram(price, 100) as price_bucket, COUNT(*) FROM t GROUP BY 1
        |  SELECT indextables_histogram(score, 10, offset => 5) as bucket, AVG(rating) FROM t GROUP BY 1
      """.stripMargin
    )
    val builder: Seq[Expression] => Expression = { args =>
      BucketFunctionParser.parseHistogram(args, Map.empty)
    }
    (funcId, info, builder)
  }

  private val rangeFunctionDesc: (FunctionIdentifier, ExpressionInfo, FunctionBuilder) = {
    val funcId = FunctionIdentifier("indextables_range")
    val info = new ExpressionInfo(
      classOf[RangeExpression].getName,
      "indextables_range",
      """
        |indextables_range(column, ranges...) - Groups data into custom-defined range buckets.
        |
        |Arguments:
        |  column - A numeric column
        |  ranges - Named ranges as 'name' => (from, to), where NULL means unbounded
        |
        |Examples:
        |  SELECT indextables_range(price, 'low' => (NULL, 50), 'mid' => (50, 200), 'high' => (200, NULL)) as tier, COUNT(*) FROM t GROUP BY 1
      """.stripMargin
    )
    val builder: Seq[Expression] => Expression = { args =>
      BucketFunctionParser.parseRange(args, Map.empty)
    }
    (funcId, info, builder)
  }
}
```

## Result Schema and Type Handling

### Output Schema Convention

Following Spark's V2 aggregate pushdown convention:
- Bucket column: `group_col_0`
- Aggregation columns: `agg_func_0`, `agg_func_1`, ...

### Type Mapping

| Bucket Type | Bucket Column Type | Notes |
|-------------|-------------------|-------|
| DateHistogram | TimestampType | Bucket start as epoch microseconds |
| Histogram | DoubleType | Bucket lower bound |
| Range | StringType | Range key/name |

| Aggregation | Output Type | Notes |
|-------------|-------------|-------|
| COUNT(*) | LongType | Bucket document count |
| COUNT(col) | LongType | Non-null count in bucket |
| SUM(int/long) | LongType | Widened to Long |
| SUM(float/double) | DoubleType | |
| MIN/MAX | Input field type | Preserves type |
| AVG | N/A | Transformed to SUM+COUNT |

## Testing Strategy

### Unit Tests

```scala
// src/test/scala/io/indextables/spark/core/BucketAggregationTest.scala

class BucketAggregationTest extends AnyFunSuite with BeforeAndAfterAll {

  test("date histogram with hourly interval") {
    val df = spark.read.format("io.indextables.spark.core.IndexTables4SparkTableProvider")
      .load(testTablePath)

    val result = df.groupBy(expr("indextables_date_histogram(timestamp, '1h')").as("hour"))
      .agg(count("*").as("count"), sum("amount").as("total"))
      .orderBy("hour")

    // Verify bucket boundaries are hourly
    // Verify aggregations are correct per bucket
  }

  test("numeric histogram with price buckets") {
    val result = df.groupBy(expr("indextables_histogram(price, 100)").as("price_bucket"))
      .agg(count("*"), avg("rating"))

    // Verify bucket boundaries
    // Verify aggregation values
  }

  test("range aggregation with named tiers") {
    val result = spark.sql("""
      SELECT indextables_range(price,
               'budget' => (NULL, 50),
               'mid' => (50, 200),
               'premium' => (200, NULL)) as tier,
             COUNT(*)
      FROM test_table
      GROUP BY 1
    """)

    // Verify tier names in results
    // Verify counts per tier
  }

  test("date histogram with filters") {
    val result = df.filter($"status" === "active")
      .groupBy(expr("indextables_date_histogram(timestamp, '1d')").as("day"))
      .agg(count("*"))

    // Verify filters are applied before bucketing
  }

  test("date histogram with sub-aggregations") {
    val result = df.groupBy(expr("indextables_date_histogram(timestamp, '1h')").as("hour"))
      .agg(
        count("*").as("count"),
        sum("amount").as("total"),
        min("price").as("min_price"),
        max("price").as("max_price")
      )

    // Verify all sub-aggregations are computed correctly
  }

  test("histogram with offset") {
    val result = df.groupBy(
        expr("indextables_histogram(score, 10, offset => 5)").as("bucket"))
      .agg(count("*"))

    // Verify buckets start at 5, 15, 25, etc.
  }

  test("date histogram with min_doc_count") {
    val result = df.groupBy(
        expr("indextables_date_histogram(timestamp, '1h', min_doc_count => 1)").as("hour"))
      .agg(count("*"))

    // Verify empty buckets are excluded
  }

  test("histogram with hard bounds") {
    val result = df.groupBy(
        expr("indextables_histogram(price, 100, hard_bounds_min => 0, hard_bounds_max => 500)").as("bucket"))
      .agg(count("*"))

    // Verify only buckets within bounds are returned
  }

  test("bucket aggregation with IndexQuery filter") {
    val result = df.filter($"content" indexquery "machine learning")
      .groupBy(expr("indextables_date_histogram(timestamp, '1d')").as("day"))
      .agg(count("*"))

    // Verify IndexQuery filter is combined with bucket aggregation
  }

  test("distributed bucket aggregation combines partial results") {
    // Create table with multiple splits
    // Verify partial results from each split are correctly combined
  }

  test("overlapping ranges throw error") {
    assertThrows[IllegalArgumentException] {
      spark.sql("""
        SELECT indextables_range(price,
                 'a' => (NULL, 100),
                 'b' => (50, 200)) as tier,
               COUNT(*)
        FROM test_table
        GROUP BY 1
      """)
    }
  }

  test("invalid interval format throws error") {
    assertThrows[IllegalArgumentException] {
      df.groupBy(expr("indextables_date_histogram(timestamp, 'invalid')"))
        .agg(count("*"))
        .collect()
    }
  }
}
```

### Integration Tests

```scala
class BucketAggregationIntegrationTest extends AnyFunSuite {

  test("end-to-end date histogram on S3") {
    // Write test data to S3
    // Read and execute bucket aggregation
    // Verify results
  }

  test("bucket aggregation with partitioned table") {
    // Create partitioned table
    // Verify bucket aggregation works across partitions
  }

  test("large scale bucket aggregation performance") {
    // Test with significant data volume
    // Verify performance is acceptable
  }
}
```

## Phase Implementation Plan

### Phase 1: Date Histogram (Week 1-2)
- [ ] Implement `DateHistogramExpression` Catalyst expression
- [ ] Add SQL function registration for `indextables_date_histogram`
- [ ] Implement detection in `ScanBuilder.pushAggregation()`
- [ ] Create `IndexTables4SparkBucketAggregateScan` with DateHistogram support
- [ ] Add comprehensive tests
- [ ] Documentation

### Phase 2: Numeric Histogram (Week 3)
- [ ] Implement `HistogramExpression` Catalyst expression
- [ ] Add SQL function registration for `indextables_histogram`
- [ ] Extend `BucketAggregateScan` for Histogram
- [ ] Add tests
- [ ] Documentation

### Phase 3: Range Aggregation (Week 4)
- [ ] Implement `RangeExpression` Catalyst expression
- [ ] Add SQL function registration for `indextables_range`
- [ ] Extend `BucketAggregateScan` for Range
- [ ] Add tests
- [ ] Documentation

### Phase 4: Polish and Future Enhancements
- [ ] Performance optimization
- [ ] Add `extended_bounds` support
- [ ] Consider auto-detection of `date_trunc()` patterns
- [ ] Add more interval formats (calendar intervals if supported by tantivy)

## Configuration Options

```scala
// No new configuration required for basic functionality.
// Bucket aggregation uses existing fast field requirements.

// Optional: Disable bucket aggregation pushdown (for debugging)
spark.indextables.aggregate.bucket.pushdown: true (default)

// Optional: Control bucket result size limit
spark.indextables.aggregate.bucket.maxBuckets: 10000 (default)
```

## Error Handling

| Error Condition | Behavior |
|-----------------|----------|
| Invalid interval format | Throw `IllegalArgumentException` with valid formats |
| Overlapping ranges | Throw `IllegalArgumentException` |
| Field not found | Throw `AnalysisException` |
| Field not a fast field | Log warning, proceed (performance may be suboptimal) |
| Bucket result exceeds limit | Truncate with warning |
| tantivy aggregation failure | Log error, return empty results |

## Future Enhancements

### Auto-Detection of Standard Expressions (Phase 2+)

Recognize standard Spark expressions and map to bucket aggregations:

```scala
// In ScanBuilder.detectBucketExpression()

// Detect date_trunc patterns
case TruncTimestamp(format, timeColumn, _) =>
  val interval = formatToInterval(format)  // "hour" -> "1h", "day" -> "1d"
  Some(DateHistogramConfig(extractFieldName(timeColumn), interval, ...))

// Detect FLOOR division patterns
case Multiply(Floor(Divide(field, Literal(divisor, _))), Literal(multiplier, _))
    if divisor == multiplier =>
  Some(HistogramConfig(extractFieldName(field), divisor.toDouble, ...))
```

### Calendar Intervals

If tantivy adds calendar interval support:
```sql
SELECT indextables_date_histogram(timestamp, '1M') as month, COUNT(*)  -- monthly
SELECT indextables_date_histogram(timestamp, '1Q') as quarter, COUNT(*)  -- quarterly
```

### Percentile/Quantile Buckets

```sql
SELECT indextables_percentile_buckets(score, 4) as quartile, COUNT(*)
FROM table
GROUP BY 1
```

## Appendix: Interval Format Reference

| Format | Description | Example |
|--------|-------------|---------|
| `1ms` | 1 millisecond | High-frequency data |
| `10ms` | 10 milliseconds | Sub-second analysis |
| `100ms` | 100 milliseconds | |
| `1s` | 1 second | Per-second metrics |
| `30s` | 30 seconds | |
| `1m` | 1 minute | Per-minute analysis |
| `5m` | 5 minutes | |
| `15m` | 15 minutes | Quarter-hourly |
| `30m` | 30 minutes | Half-hourly |
| `1h` | 1 hour | Hourly trends |
| `6h` | 6 hours | Quarter-daily |
| `12h` | 12 hours | Half-daily |
| `1d` | 1 day | Daily patterns |
| `7d` | 7 days | Weekly |

**Note:** Calendar intervals (month, quarter, year) are NOT supported due to tantivy limitations.
