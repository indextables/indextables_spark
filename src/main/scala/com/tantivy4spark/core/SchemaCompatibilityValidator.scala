/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tantivy4spark.core

import org.apache.spark.sql.types.{StructType, StructField, DataType}
import com.tantivy4spark.transaction.TransactionLog
import scala.util.{Try, Success, Failure}

case class SchemaValidationResult(
    isCompatible: Boolean,
    errors: List[String] = List.empty,
    warnings: List[String] = List.empty
)

object SchemaCompatibilityValidator {
  
  def validateSchemaCompatibility(
      existingSchema: StructType,
      newSchema: StructType,
      enforceStrict: Boolean,
      allowSchemaEvolution: Boolean
  ): SchemaValidationResult = {
    doValidateSchemaCompatibility(existingSchema, newSchema, enforceStrict, allowSchemaEvolution)
  }
  
  def validateSchemaForWrite(
      datasetPath: String, 
      newSchema: StructType, 
      options: Map[String, String]
  ): SchemaValidationResult = {
    
    val enforceStrict = options.getOrElse("schema.compatibility.strict", "false").toBoolean
    val allowSchemaEvolution = options.getOrElse("schema.evolution.enabled", "true").toBoolean
    
    // Try to get existing schema from transaction log
    val txnLog = new TransactionLog(datasetPath, options)
    txnLog.inferSchemaFromTransactionLog(datasetPath) match {
      case Some(existingSchema) =>
        doValidateSchemaCompatibility(existingSchema, newSchema, enforceStrict, allowSchemaEvolution)
      case None =>
        // No existing schema found, any schema is valid for new datasets
        SchemaValidationResult(isCompatible = true, warnings = List("No existing schema found, creating new dataset"))
    }
  }
  
  private def doValidateSchemaCompatibility(
      existingSchema: StructType, 
      newSchema: StructType,
      enforceStrict: Boolean,
      allowSchemaEvolution: Boolean
  ): SchemaValidationResult = {
    
    val errors = scala.collection.mutable.ListBuffer[String]()
    val warnings = scala.collection.mutable.ListBuffer[String]()
    
    val existingFields = existingSchema.fields.map(f => f.name -> f).toMap
    val newFields = newSchema.fields.map(f => f.name -> f).toMap
    
    // Check for removed fields
    val removedFields = existingFields.keySet -- newFields.keySet
    if (removedFields.nonEmpty) {
      if (enforceStrict) {
        errors += s"Cannot remove fields: ${removedFields.mkString(", ")}"
      } else {
        warnings += s"Fields removed from schema: ${removedFields.mkString(", ")}"
      }
    }
    
    // Check for type changes in common fields
    val commonFields = existingFields.keySet.intersect(newFields.keySet)
    commonFields.foreach { fieldName =>
      val existingField = existingFields(fieldName)
      val newField = newFields(fieldName)
      
      if (!areTypesCompatible(existingField.dataType, newField.dataType)) {
        if (enforceStrict) {
          errors += s"Incompatible type change for field '$fieldName': ${existingField.dataType} -> ${newField.dataType}"
        } else if (allowSchemaEvolution && isTypePromotionSafe(existingField.dataType, newField.dataType)) {
          warnings += s"Safe type promotion for field '$fieldName': ${existingField.dataType} -> ${newField.dataType}"
        } else {
          errors += s"Unsafe type change for field '$fieldName': ${existingField.dataType} -> ${newField.dataType}"
        }
      }
      
      // Check nullability changes
      if (existingField.nullable && !newField.nullable) {
        errors += s"Cannot change field '$fieldName' from nullable to non-nullable"
      } else if (!existingField.nullable && newField.nullable) {
        warnings += s"Field '$fieldName' changed from non-nullable to nullable (safe change)"
      }
    }
    
    // Check for new fields
    val newlyAddedFields = newFields.keySet -- existingFields.keySet
    if (newlyAddedFields.nonEmpty) {
      newlyAddedFields.foreach { fieldName =>
        val newField = newFields(fieldName)
        if (!newField.nullable) {
          errors += s"New non-nullable field '$fieldName' cannot be added to existing dataset"
        } else {
          warnings += s"New nullable field added: '$fieldName'"
        }
      }
    }
    
    SchemaValidationResult(
      isCompatible = errors.isEmpty,
      errors = errors.toList,
      warnings = warnings.toList
    )
  }
  
  private def areTypesCompatible(existingType: DataType, newType: DataType): Boolean = {
    if (existingType == newType) {
      true
    } else {
      isTypePromotionSafe(existingType, newType)
    }
  }
  
  private def isTypePromotionSafe(fromType: DataType, toType: DataType): Boolean = {
    import org.apache.spark.sql.types._
    
    (fromType, toType) match {
      // Numeric promotions
      case (ByteType, ShortType | IntegerType | LongType | FloatType | DoubleType) => true
      case (ShortType, IntegerType | LongType | FloatType | DoubleType) => true
      case (IntegerType, LongType | FloatType | DoubleType) => true
      case (LongType, FloatType | DoubleType) => true
      case (FloatType, DoubleType) => true
      
      // Date/Time promotions
      case (DateType, TimestampType) => true
      
      // String is generally compatible with most types for reading
      case (_, StringType) => true
      
      case _ => false
    }
  }
}