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

package io.indextables.spark.filters

import java.util.UUID

import scala.collection.concurrent
import scala.collection.mutable

/**
 * Registry for IndexQuery information that uses query-scoped keys for distributed safety. Instead of relying on
 * ThreadLocal, we use query IDs to scope the IndexQuery information.
 */
object IndexQueryRegistry {

  // Use concurrent maps for thread-safety
  private val queryIndexQueries   = concurrent.TrieMap[String, mutable.Buffer[IndexQueryFilter]]()
  private val queryIndexQueryAlls = concurrent.TrieMap[String, mutable.Buffer[IndexQueryAllFilter]]()

  // Track the current query ID per thread (this is safe since query planning happens on driver)
  private val currentQueryId = new ThreadLocal[String]()

  def startNewQuery(): String = {
    val queryId = UUID.randomUUID().toString
    currentQueryId.set(queryId)
    queryId
  }

  def setCurrentQuery(queryId: String): Unit = {
    currentQueryId.set(queryId)
  }

  def getCurrentQueryId(): Option[String] =
    Option(currentQueryId.get())

  def registerIndexQuery(columnName: String, queryString: String): Unit =
    getCurrentQueryId() match {
      case Some(queryId) =>
        val filters = queryIndexQueries.getOrElseUpdate(queryId, mutable.Buffer.empty)
        filters += IndexQueryFilter(columnName, queryString)
      case None =>
        // Silently skip if no current query ID
    }

  def registerIndexQueryAll(queryString: String): Unit =
    getCurrentQueryId() match {
      case Some(queryId) =>
        val filters = queryIndexQueryAlls.getOrElseUpdate(queryId, mutable.Buffer.empty)
        filters += IndexQueryAllFilter(queryString)
      case None =>
        // Silently skip if no current query ID
    }

  def getIndexQueriesForQuery(queryId: String): Seq[Any] = {
    val indexQueries   = queryIndexQueries.getOrElse(queryId, mutable.Buffer.empty).toSeq
    val indexQueryAlls = queryIndexQueryAlls.getOrElse(queryId, mutable.Buffer.empty).toSeq
    indexQueries ++ indexQueryAlls
  }

  def clearQuery(queryId: String): Unit = {
    queryIndexQueries.remove(queryId)
    queryIndexQueryAlls.remove(queryId)
  }

  def clearCurrentQuery(): Unit = {
    getCurrentQueryId() foreach clearQuery
    currentQueryId.remove()
  }
}
