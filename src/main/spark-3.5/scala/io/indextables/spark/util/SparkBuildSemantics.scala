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

/**
 * Spark-version build-call semantics (spark-3.5 variant).
 *
 * Spark 3.5 calls ScanBuilder.build() twice for a failing query: once on an explain/logging path (its exception is
 * swallowed) and once on the body-execution path (its exception reaches the user). Stale IndexQuery cleanup must
 * therefore only happen on the second call, so the first call's failure doesn't erase the queries the second call
 * needs in order to raise the same user-visible error.
 */
object SparkBuildSemantics {
  val singleBuildPerQuery: Boolean = false
}
