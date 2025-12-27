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

package io.indextables.spark.util

import org.apache.spark.sql.types.StructType

import org.slf4j.LoggerFactory

/**
 * Utility object for validating Spark schemas.
 *
 * Provides centralized schema validation logic used across write operations.
 */
object SchemaValidator {

  private val logger = LoggerFactory.getLogger(this.getClass)

  /**
   * Validates that a schema does not contain duplicate column names.
   *
   * @param schema
   *   The schema to validate
   * @throws IllegalArgumentException
   *   if duplicate column names are found
   */
  def validateNoDuplicateColumns(schema: StructType): Unit = {
    val fieldNames = schema.fieldNames
    val duplicates = fieldNames.groupBy(identity).filter(_._2.length > 1).keys.toSeq

    if (duplicates.nonEmpty) {
      val duplicateList = duplicates.mkString(", ")
      val errorMsg = s"Schema contains duplicate column names: [$duplicateList]. " +
        s"Please ensure all column names are unique. Duplicate columns can cause JVM crashes."
      logger.error(errorMsg)
      throw new IllegalArgumentException(errorMsg)
    }

    // Also check for case-insensitive duplicates (warn only, don't fail)
    val lowerCaseNames            = fieldNames.map(_.toLowerCase)
    val caseInsensitiveDuplicates = lowerCaseNames.groupBy(identity).filter(_._2.length > 1).keys.toSeq

    if (caseInsensitiveDuplicates.nonEmpty) {
      val originalNames =
        caseInsensitiveDuplicates.flatMap(lower => fieldNames.filter(_.toLowerCase == lower)).distinct.mkString(", ")
      logger.warn(
        s"Schema contains columns that differ only in case: [$originalNames]. " +
          s"This may cause issues with case-insensitive storage systems."
      )
    }
  }
}
