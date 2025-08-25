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

package com.tantivy4spark.sql

import org.apache.spark.sql.SparkSessionExtensions

/**
 * Spark SQL extension for Tantivy4Spark to support custom SQL commands.
 *
 * Usage:
 * {{{
 *    val spark = SparkSession
 *       .builder()
 *       .appName("...")
 *       .master("...")
 *       .config("spark.sql.extensions", "com.tantivy4spark.sql.Tantivy4SparkSessionExtension")
 *       .getOrCreate()
 * }}}
 */
class Tantivy4SparkSessionExtension extends (SparkSessionExtensions => Unit) {
  
  override def apply(extensions: SparkSessionExtensions): Unit = {
    // Register our SQL parser for Tantivy4Spark commands
    extensions.injectParser { (session, parser) =>
      new Tantivy4SparkSqlParser(parser)
    }
  }
}