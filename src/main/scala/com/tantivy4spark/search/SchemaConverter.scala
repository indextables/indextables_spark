package com.tantivy4spark.search

import com.tantivy4spark.util.{JsonUtil, TypeConversionUtil}
import org.apache.spark.sql.types._

object SchemaConverter {

  def sparkToTantivySchema(schema: StructType): String = {
    val fields = schema.fields.map { field =>
      val tantivyType = TypeConversionUtil.sparkTypeToTantivyType(field.dataType)
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

    JsonUtil.mapper.writeValueAsString(schemaMap)
  }
}