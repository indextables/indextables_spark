package com.tantivy4spark.search

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.sql.types._

object SchemaConverter {
  
  private val mapper = new ObjectMapper().registerModule(DefaultScalaModule)

  def sparkToTantivySchema(schema: StructType): String = {
    val fields = schema.fields.map { field =>
      val tantivyType = sparkTypeToTantivyType(field.dataType)
      Map[String, Any](
        "name" -> field.name,
        "type" -> tantivyType,
        "indexed" -> true,
        "stored" -> true
      )
    }

    val schemaMap = Map(
      "fields" -> fields
    )

    mapper.writeValueAsString(schemaMap)
  }

  private def sparkTypeToTantivyType(dataType: DataType): String = dataType match {
    case StringType => "text"
    case IntegerType => "i64"
    case LongType => "i64"
    case FloatType => "f64"
    case DoubleType => "f64"
    case BooleanType => "i64" // Store as 0/1
    case BinaryType => "bytes"
    case TimestampType => "i64" // Store as epoch millis
    case DateType => "i64" // Store as days since epoch
    case _ => "text" // Fallback to text for complex types
  }
}