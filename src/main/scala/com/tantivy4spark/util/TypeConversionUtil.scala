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

package com.tantivy4spark.util

import org.apache.spark.sql.types._

object TypeConversionUtil {

  def extractBasicValue(getValue: DataType => Any, dataType: DataType): Any = dataType match {
    case StringType => getValue(StringType)
    case IntegerType => getValue(IntegerType)  
    case LongType => getValue(LongType)
    case FloatType => getValue(FloatType)
    case DoubleType => getValue(DoubleType)
    case BooleanType => getValue(BooleanType)
    case BinaryType => getValue(BinaryType)
    case TimestampType => getValue(TimestampType)
    case DateType => getValue(DateType)
    case _ => getValue(dataType)
  }

  def sparkTypeToTantivyType(dataType: DataType): String = dataType match {
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