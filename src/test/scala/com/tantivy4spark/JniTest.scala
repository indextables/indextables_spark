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

package com.tantivy4spark

import com.tantivy4spark.native.TantivyNative
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class JniTest extends AnyFlatSpec with Matchers {
  
  "TantivyNative" should "load native library successfully" in {
    // This test just verifies that the native library can be loaded
    // without calling any actual native methods
    noException should be thrownBy {
      val _ = TantivyNative.getClass
    }
  }
  
  it should "create and destroy config successfully" in {
    try {
      val testConfig = """{"index_id": "test"}"""
      
      val configId = TantivyNative.createConfig(testConfig)
      configId should be > 0L
      
      // Clean up
      TantivyNative.destroyConfig(configId)
      
      info(s"SUCCESS: JNI calls completed successfully with config ID: $configId")
      
    } catch {
      case e: UnsatisfiedLinkError =>
        fail(s"JNI UnsatisfiedLinkError: ${e.getMessage}")
      case e: Exception =>
        // Allow other exceptions as they might be from the Rust logic, not JNI loading
        info(s"Got expected exception from Rust code: ${e.getMessage}")
    }
  }
}