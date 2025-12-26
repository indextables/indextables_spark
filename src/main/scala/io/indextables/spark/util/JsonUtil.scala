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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

/**
 * Centralized JSON utilities using a shared ObjectMapper instance.
 *
 * Provides consistent JSON handling across the codebase and avoids creating multiple ObjectMapper instances.
 */
object JsonUtil {

  /** Shared ObjectMapper instance with Scala module registered. */
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /**
   * Parse a JSON string array to a Scala Seq.
   *
   * @param json
   *   JSON array string like ["a", "b", "c"]
   * @return
   *   Seq of strings
   */
  def parseStringArray(json: String): Seq[String] =
    mapper.readValue(json, classOf[Array[String]]).toSeq

  /**
   * Parse a JSON value to a specific class type.
   *
   * @param json
   *   JSON string
   * @param clazz
   *   Target class type
   * @return
   *   Parsed value
   */
  def parseAs[T](json: String, clazz: Class[T]): T =
    mapper.readValue(json, clazz)

  /**
   * Convert an object to a JSON string.
   *
   * @param value
   *   Object to convert
   * @return
   *   JSON string representation
   */
  def toJson(value: Any): String =
    mapper.writeValueAsString(value)
}
