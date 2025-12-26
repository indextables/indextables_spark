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
 * Centralized JSON utilities using shared ObjectMapper instances.
 *
 * Provides consistent JSON handling across the codebase and avoids creating multiple ObjectMapper instances.
 */
object JsonUtil {

  /** Shared ObjectMapper instance with Scala module registered for Scala types. */
  val mapper: ObjectMapper = new ObjectMapper().registerModule(DefaultScalaModule)

  /** Plain ObjectMapper without Scala module for Java collections (Maps, Lists). */
  private val javaMapper: ObjectMapper = new ObjectMapper()

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
   * Uses plain ObjectMapper for Java collections to avoid DefaultScalaModule interference.
   *
   * @param value
   *   Object to convert
   * @return
   *   JSON string representation
   */
  def toJson(value: Any): String =
    value match {
      case _: java.util.Map[_, _] | _: java.util.List[_] =>
        javaMapper.writeValueAsString(value)
      case _ =>
        mapper.writeValueAsString(value)
    }

  /**
   * Convert a Java Map to a JSON string using plain ObjectMapper.
   *
   * @param map
   *   Java Map to convert
   * @return
   *   JSON string representation
   */
  def toJsonJava(map: java.util.Map[_, _]): String =
    javaMapper.writeValueAsString(map)

  /**
   * Parse a JSON string to a Java Map using plain ObjectMapper.
   *
   * @param json
   *   JSON string
   * @param clazz
   *   Target class type
   * @return
   *   Parsed Java Map
   */
  def parseAsJava[T](json: String, clazz: Class[T]): T =
    javaMapper.readValue(json, clazz)
}
