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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LibraryLoadTest extends AnyFlatSpec with Matchers {
  
  "Native library loading" should "work correctly" in {
    val resourcePath = "/native/libtantivy_jni.dylib"
    val inputStream = getClass.getResourceAsStream(resourcePath)
    
    println(s"Checking resource path: $resourcePath")
    println(s"InputStream is null: ${inputStream == null}")
    
    if (inputStream != null) {
      println(s"InputStream available: ${inputStream.available()} bytes")
      inputStream.close()
    }
    
    // Try to load the library directly
    try {
      val tempFile = java.io.File.createTempFile("test_tantivy_jni", ".dylib")
      tempFile.deleteOnExit()
      
      val resourceStream = getClass.getResourceAsStream(resourcePath)
      if (resourceStream != null) {
        val outputStream = new java.io.FileOutputStream(tempFile)
        try {
          val buffer = new Array[Byte](8192)
          var bytesRead = resourceStream.read(buffer)
          while (bytesRead != -1) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = resourceStream.read(buffer)
          }
        } finally {
          resourceStream.close()
          outputStream.close()
        }
        
        println(s"Temp file created: ${tempFile.getAbsolutePath}")
        println(s"Temp file size: ${tempFile.length()} bytes")
        
        // Try to load the library
        System.load(tempFile.getAbsolutePath)
        println("Library loaded successfully!")
      }
    } catch {
      case e: Exception =>
        println(s"Failed to load library: ${e.getMessage}")
        e.printStackTrace()
        fail(s"Library loading failed: ${e.getMessage}")
    }
  }
}