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

package io.indextables.spark.core

import org.scalatest.funsuite.AnyFunSuite

/** Test to discover the correct methods in SupportsPushDownAggregates interface. */
class SupportsPushDownAggregatesTest extends AnyFunSuite {

  test("discover SupportsPushDownAggregates methods") {
    try {
      val clazz = Class.forName("org.apache.spark.sql.connector.read.SupportsPushDownAggregates")
      println(s"✅ SupportsPushDownAggregates available: ${clazz.getName}")

      val methods = clazz.getMethods.filter(_.getDeclaringClass == clazz)
      println(s"SupportsPushDownAggregates methods:")
      methods.foreach { method =>
        val paramTypes = method.getParameterTypes.map(_.getSimpleName).mkString(", ")
        println(s"  - ${method.getName}($paramTypes): ${method.getReturnType.getSimpleName}")
      }
    } catch {
      case e: Exception => println(s"❌ Error loading SupportsPushDownAggregates: ${e.getMessage}")
    }
  }
}
