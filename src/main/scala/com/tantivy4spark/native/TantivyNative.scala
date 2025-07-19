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

package com.tantivy4spark.native

class TantivyNative {
  // Native method declarations (these map to the JNI functions in Rust)
  @native def createConfig(configJson: String): Long
  @native def destroyConfig(configId: Long): Unit
  
  @native def createSearchEngine(configId: Long, indexPath: String): Long  
  @native def search(engineId: Long, query: String, maxHits: Long): String
  @native def destroySearchEngine(engineId: Long): Unit
  
  @native def createIndexWriter(configId: Long, indexPath: String, schemaJson: String): Long
  @native def indexDocument(writerId: Long, documentJson: String): Boolean
  @native def commitIndex(writerId: Long): Boolean
  @native def destroyIndexWriter(writerId: Long): Unit
}

object TantivyNative {
  
  // Determine the library name based on platform
  private val libraryName = if (System.getProperty("os.name").toLowerCase.contains("windows")) {
    "tantivy_jni.dll"
  } else if (System.getProperty("os.name").toLowerCase.contains("mac")) {
    "libtantivy_jni.dylib"
  } else {
    "libtantivy_jni.so"
  }
  
  // Load the native library from classpath
  private def loadNativeLibrary(): Unit = {
    try {
      // First try to load from system library path
      System.loadLibrary("tantivy_jni")
    } catch {
      case _: UnsatisfiedLinkError =>
        // Extract from classpath and load
        val resourcePath = s"/native/$libraryName"
        val inputStream = getClass.getResourceAsStream(resourcePath)
        
        if (inputStream == null) {
          throw new RuntimeException(s"Native library not found in resources: $resourcePath")
        }
        
        val extension = if (libraryName.endsWith(".dll")) ".dll" 
                       else if (libraryName.endsWith(".dylib")) ".dylib" 
                       else ".so"
        val tempFile = java.io.File.createTempFile("tantivy_jni", extension)
        tempFile.deleteOnExit()
        
        val outputStream = new java.io.FileOutputStream(tempFile)
        try {
          val buffer = new Array[Byte](8192)
          var bytesRead = inputStream.read(buffer)
          while (bytesRead != -1) {
            outputStream.write(buffer, 0, bytesRead)
            bytesRead = inputStream.read(buffer)
          }
        } finally {
          inputStream.close()
          outputStream.close()
        }
        
        System.load(tempFile.getAbsolutePath)
    }
  }
  
  // Load the library when the object is first accessed
  loadNativeLibrary()
  
  // Singleton instance for easy access
  private val instance = new TantivyNative()
  
  // Convenience methods that delegate to the instance
  def createConfig(configJson: String): Long = instance.createConfig(configJson)
  def destroyConfig(configId: Long): Unit = instance.destroyConfig(configId)
  
  def createSearchEngine(configId: Long, indexPath: String): Long = instance.createSearchEngine(configId, indexPath)
  def search(engineId: Long, query: String, maxHits: Long = 100): String = instance.search(engineId, query, maxHits)
  def destroySearchEngine(engineId: Long): Unit = instance.destroySearchEngine(engineId)
  
  def createIndexWriter(configId: Long, indexPath: String, schemaJson: String): Long = instance.createIndexWriter(configId, indexPath, schemaJson)
  def indexDocument(writerId: Long, documentJson: String): Boolean = instance.indexDocument(writerId, documentJson)
  def commitIndex(writerId: Long): Boolean = instance.commitIndex(writerId)
  def destroyIndexWriter(writerId: Long): Unit = instance.destroyIndexWriter(writerId)
}