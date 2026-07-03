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

package org.apache.spark.sql.indextables

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.Column

/**
 * Test-only Column <-> catalyst Expression conversions (spark-3.5 variant). In Spark 3.5, Column wraps an Expression
 * directly.
 */
object ColumnCompat {
  def expr(c: Column): Expression   = c.expr
  def column(e: Expression): Column = new Column(e)
}
