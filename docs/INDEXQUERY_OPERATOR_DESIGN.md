# IndexQuery Operator Design Document

## Overview

This document outlines the design for implementing a custom `indexquery` SQL operator in Tantivy4Spark that enables direct Tantivy query syntax in Spark SQL queries. The operator will be pushed down to the data source for native execution using tantivy4java's `SplitIndex.parseQuery()` method.

## Requirements

The `indexquery` operator should support SQL syntax like:
```sql
SELECT * FROM my_tantivy_table 
WHERE column_one indexquery 'TERM1 and TERM2'
```

### Key Requirements:
1. **SQL Integration**: Custom binary operator `indexquery` that appears between a column reference and a query string
2. **Filter Pushdown**: The operator must be recognized and pushed down to the Tantivy data source 
3. **Native Execution**: Queries are executed using `SplitIndex.parseQuery()` for optimal performance
4. **Error Handling**: Graceful handling of invalid query syntax and missing columns
5. **Type Safety**: Validate that the left operand is a valid column reference

## Architecture Overview

The implementation involves four key components:

```
SQL Query Parser → Custom Expression → Filter Pushdown → Native Query Execution
    ↓                    ↓                 ↓                     ↓
Catalyst AST      IndexQueryExpression   Tantivy Filter    parseQuery() call
```

## Implementation Design

### 1. Custom Catalyst Expression

Create a new `IndexQueryExpression` that extends Spark's `BinaryExpression`:

**File**: `src/main/scala/com/tantivy4spark/expressions/IndexQueryExpression.scala`

```scala
package com.tantivy4spark.expressions

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.{CodegenContext, ExprCode}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Expression for the INDEXQUERY operator that represents a direct Tantivy query.
 * 
 * Usage: column_name indexquery 'query_string'
 * 
 * This expression is primarily designed for pushdown to the Tantivy data source.
 * When evaluated in Spark (fallback), it returns true for all rows since the 
 * actual filtering should happen at the data source level.
 */
case class IndexQueryExpression(
    left: Expression,   // Column reference
    right: Expression   // Query string literal
) extends BinaryExpression with Predicate {

  override def inputTypes: Seq[DataType] = Seq(AnyDataType, StringType)
  
  override def dataType: DataType = BooleanType
  
  override def symbol: String = "indexquery"
  
  override def prettyName: String = "indexquery"
  
  override def sql: String = s"(${left.sql} indexquery ${right.sql})"
  
  // For pushdown, we primarily care about the structure, not evaluation
  override def nullSafeEval(leftValue: Any, rightValue: Any): Any = {
    // This should rarely be called since the expression should be pushed down
    // If called, return true as a safe fallback (filtering happens at source)
    true
  }
  
  override protected def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
    // Code generation for the rare case this isn't pushed down
    ev.copy(code = code"boolean ${ev.value} = true;", isNull = "false")
  }
  
  /**
   * Extract the column name from the left expression.
   */
  def getColumnName: Option[String] = left match {
    case attr: AttributeReference => Some(attr.name)
    case _ => None
  }
  
  /**
   * Extract the query string from the right expression.
   */
  def getQueryString: Option[String] = right match {
    case Literal(value: UTF8String, StringType) => Some(value.toString)
    case Literal(value: String, StringType) => Some(value)
    case _ => None
  }
}
```

### 2. Custom Filter for Pushdown

Create a custom Spark Filter that can be pushed down to the data source:

**File**: `src/main/scala/com/tantivy4spark/filters/IndexQueryFilter.scala`

```scala
package com.tantivy4spark.filters

import org.apache.spark.sql.sources.Filter

/**
 * Custom filter representing an indexquery operation for pushdown to Tantivy data source.
 * 
 * This filter encapsulates:
 * - The column name to query against
 * - The raw Tantivy query string to execute
 */
case class IndexQueryFilter(
    column: String,
    queryString: String
) extends Filter {
  
  override def toString: String = s"IndexQuery($column, '$queryString')"
  
  /**
   * References returns the set of column names that this filter references.
   */
  override def references: Array[String] = Array(column)
}
```

### 3. Enhanced SQL Parser

Extend the existing `Tantivy4SparkSqlParser` to recognize the `indexquery` operator:

**File**: `src/main/scala/com/tantivy4spark/sql/Tantivy4SparkSqlParser.scala`

```scala
package com.tantivy4spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.types.{DataType, StructType}
import org.apache.spark.sql.catalyst.parser.extensions.SqlExtensions
import com.tantivy4spark.expressions.IndexQueryExpression

/**
 * Enhanced SQL parser for Tantivy4Spark with indexquery operator support.
 */
class Tantivy4SparkSqlParser(delegate: ParserInterface) extends ParserInterface {

  override def parsePlan(sqlText: String): LogicalPlan = {
    val trimmed = sqlText.trim.toUpperCase
    
    if (trimmed == "FLUSH TANTIVY4SPARK SEARCHER CACHE") {
      FlushTantivyCacheCommand()
    } else {
      delegate.parsePlan(sqlText)
    }
  }

  override def parseExpression(sqlText: String): Expression = {
    // Check for indexquery operator pattern
    val indexQueryPattern = """(.+?)\s+indexquery\s+(.+)""".r
    
    sqlText.trim match {
      case indexQueryPattern(leftExpr, rightExpr) =>
        val left = delegate.parseExpression(leftExpr.trim)
        val right = delegate.parseExpression(rightExpr.trim)
        IndexQueryExpression(left, right)
      case _ =>
        delegate.parseExpression(sqlText)
    }
  }

  // ... other methods delegate to parent
}
```

### 4. Enhanced Filter Pushdown

Update `FiltersToQueryConverter` to handle `IndexQueryFilter`:

**File**: `src/main/scala/com/tantivy4spark/core/FiltersToQueryConverter.scala`

Add to the `convertFilterToQuery` method:

```scala
// Add to the pattern matching in convertFilterToQuery
case indexQuery: IndexQueryFilter =>
  queryLog(s"Creating IndexQuery: ${indexQuery.column} indexquery '${indexQuery.queryString}'")
  
  // Validate that the field exists in the schema
  val fieldExists = try {
    val fieldInfo = schema.getFieldInfo(indexQuery.column)
    true
  } catch {
    case _: Exception =>
      logger.warn(s"IndexQuery field '${indexQuery.column}' not found in schema, skipping")
      false
  }
  
  if (!fieldExists) {
    // Return match-all query if field doesn't exist (graceful degradation)
    Query.allQuery()
  } else {
    // Use parseQuery with the specified field
    val fieldNames = List(indexQuery.column).asJava
    queryLog(s"Executing parseQuery: '${indexQuery.queryString}' on field '${indexQuery.column}'")
    
    withTemporaryIndex(schema) { index =>
      try {
        index.parseQuery(indexQuery.queryString, fieldNames)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to parse indexquery '${indexQuery.queryString}': ${e.getMessage}")
          // Fallback to match-all on parse failure
          Query.allQuery()
      }
    }
  }
```

### 5. Enhanced Scan Builder

Update `Tantivy4SparkScanBuilder` to recognize and support `IndexQueryFilter`:

**File**: `src/main/scala/com/tantivy4spark/core/Tantivy4SparkScanBuilder.scala`

```scala
// Add to the isSupportedFilter method
private def isSupportedFilter(filter: Filter): Boolean = {
  import org.apache.spark.sql.sources._
  import com.tantivy4spark.filters.IndexQueryFilter
  
  filter match {
    // ... existing cases ...
    case _: IndexQueryFilter => true  // Add support for IndexQueryFilter
    case _ => false
  }
}
```

### 6. Expression to Filter Conversion

Create a utility to convert `IndexQueryExpression` to `IndexQueryFilter` during pushdown:

**File**: `src/main/scala/com/tantivy4spark/util/ExpressionUtils.scala`

```scala
package com.tantivy4spark.util

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.sources.Filter
import com.tantivy4spark.expressions.IndexQueryExpression
import com.tantivy4spark.filters.IndexQueryFilter

object ExpressionUtils {
  
  /**
   * Convert Catalyst expressions to Spark SQL filters for pushdown.
   */
  def expressionToFilter(expr: Expression): Option[Filter] = {
    expr match {
      case IndexQueryExpression(left, right) =>
        for {
          columnName <- extractColumnName(left)
          queryString <- extractStringLiteral(right)
        } yield IndexQueryFilter(columnName, queryString)
      case _ => None
    }
  }
  
  private def extractColumnName(expr: Expression): Option[String] = {
    // Implementation to extract column name from expression
  }
  
  private def extractStringLiteral(expr: Expression): Option[String] = {
    // Implementation to extract string literal from expression
  }
}
```

## Integration Points

### 1. Spark Session Extensions

To integrate the custom parser, we need to register it with Spark:

**File**: `src/main/scala/com/tantivy4spark/extensions/Tantivy4SparkExtensions.scala`

```scala
package com.tantivy4spark.extensions

import org.apache.spark.sql.SparkSessionExtensions
import com.tantivy4spark.sql.Tantivy4SparkSqlParser

class Tantivy4SparkExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { (session, parser) =>
      new Tantivy4SparkSqlParser(parser)
    }
  }
}
```

### 2. Service Registration

**File**: `src/main/resources/META-INF/services/org.apache.spark.sql.SparkSessionExtensions`

```
com.tantivy4spark.extensions.Tantivy4SparkExtensions
```

### 3. Usage Configuration

Users would configure their SparkSession to use the extensions:

```scala
val spark = SparkSession.builder()
  .appName("Tantivy4Spark with IndexQuery")
  .config("spark.sql.extensions", "com.tantivy4spark.extensions.Tantivy4SparkExtensions")
  .getOrCreate()
```

## Usage Examples

### Basic Usage
```sql
SELECT * FROM my_table WHERE title indexquery 'machine learning'
```

### Complex Queries
```sql
SELECT id, title, score FROM articles 
WHERE content indexquery 'spark AND (streaming OR batch)'
  AND publish_date > '2023-01-01'
```

### Multiple IndexQuery Conditions
```sql
SELECT * FROM documents 
WHERE title indexquery 'spark sql' 
  AND content indexquery '(performance OR optimization) AND NOT deprecated'
```

## Error Handling

### 1. Invalid Query Syntax
- Parser errors in tantivy4java are caught and logged
- Fallback to match-all query for graceful degradation
- Warning logged with original query string

### 2. Missing Columns
- Schema validation before calling parseQuery
- Graceful fallback if column doesn't exist
- Detailed error logging for debugging

### 3. Type Mismatches
- Left operand must be a column reference
- Right operand must be a string literal
- Compilation errors for invalid types

## Testing Strategy

### 1. Unit Tests
- `IndexQueryExpressionTest`: Expression behavior and validation
- `IndexQueryFilterTest`: Filter properties and serialization
- `FiltersToQueryConverterTest`: Query conversion logic

### 2. Integration Tests
- SQL parsing with indexquery operator
- End-to-end filter pushdown
- Error handling scenarios

### 3. Performance Tests
- Query performance vs. traditional filters
- Pushdown effectiveness
- Memory usage with complex queries

**File**: `src/test/scala/com/tantivy4spark/expressions/IndexQueryExpressionTest.scala`

```scala
class IndexQueryExpressionTest extends SparkFunSuite {
  
  test("IndexQueryExpression should extract column name correctly") {
    val column = AttributeReference("title", StringType, nullable = true)()
    val query = Literal("spark AND sql")
    val expr = IndexQueryExpression(column, query)
    
    assert(expr.getColumnName.contains("title"))
    assert(expr.getQueryString.contains("spark AND sql"))
  }
  
  test("IndexQueryExpression should handle invalid operands") {
    // Test with non-column left operand
    // Test with non-string right operand
  }
}
```

## Performance Considerations

### 1. Pushdown Effectiveness
- The indexquery operator bypasses Spark's query planning for the pushed-down portion
- Direct use of Tantivy's native query parser provides optimal performance
- Reduced data transfer between storage and Spark engine

### 2. Query Complexity
- Complex queries are handled natively by Tantivy
- Boolean logic, phrase queries, and wildcards work at native speed
- No overhead from Spark's expression evaluation

### 3. Memory Usage
- Temporary index creation for parseQuery is lightweight
- Query objects are efficiently cached by tantivy4java
- Minimal serialization overhead for filter pushdown

## Migration and Compatibility

### 1. Backward Compatibility
- Existing queries continue to work unchanged
- New operator is additive, doesn't break existing functionality
- Can be enabled/disabled via configuration

### 2. Gradual Adoption
- Users can mix traditional filters with indexquery operators
- Performance benefits are immediate for adopted queries
- Easy migration path from string-based search patterns

## Security Considerations

### 1. Query Injection
- Tantivy's query parser is designed to handle arbitrary input safely
- No SQL injection risk since queries are executed against the search index
- Input validation at the expression level

### 2. Resource Usage
- Query complexity limits can be enforced at the Tantivy level
- Timeouts prevent runaway queries
- Memory limits protect against resource exhaustion

## Future Enhancements

### 1. Query Builder Support
- DataFrame API support: `df.filter(col("title").indexquery("spark SQL"))`
- Type-safe query builders
- IDE integration for query syntax highlighting

### 2. Advanced Features
- Query result highlighting
- Explain query functionality 
- Query performance metrics
- Custom scoring functions

### 3. Optimization Opportunities
- Query caching across multiple calls
- Index warming for frequently accessed fields
- Parallel query execution for complex boolean queries

---

## Implementation Checklist

- [ ] Create `IndexQueryExpression` class
- [ ] Create `IndexQueryFilter` class  
- [ ] Enhance `Tantivy4SparkSqlParser` for operator recognition
- [ ] Update `FiltersToQueryConverter` with IndexQuery support
- [ ] Update `Tantivy4SparkScanBuilder` filter support
- [ ] Create `ExpressionUtils` for conversion logic
- [ ] Implement `Tantivy4SparkExtensions` for registration
- [ ] Add service registration files
- [ ] Write comprehensive tests
- [ ] Update documentation and examples
- [ ] Performance benchmarking

This design provides a robust foundation for implementing the `indexquery` operator while maintaining compatibility with existing Tantivy4Spark functionality and following Spark's extension patterns.