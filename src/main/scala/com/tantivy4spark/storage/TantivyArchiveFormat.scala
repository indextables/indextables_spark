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


package com.tantivy4spark.storage

import java.io.{DataOutputStream, OutputStream}
import java.nio.ByteBuffer
import scala.collection.mutable
import org.slf4j.LoggerFactory

/**
 * Tantivy Archive Format (.tnt4s) specification:
 * 
 * File structure:
 * [Component 1 Data][Component 2 Data]...[Component N Data][Footer]
 * 
 * Footer structure:
 * [Table of Contents][Footer Size (8 bytes)][Magic Number (8 bytes)]
 * 
 * Table of Contents structure:
 * [Number of Components (4 bytes)]
 * [Component 1 Entry: Name Length (4 bytes) + Name (bytes) + Offset (8 bytes) + Size (8 bytes)]
 * [Component 2 Entry: ...]
 * ...
 * 
 * This allows efficient random access to any component without reading the entire archive.
 */

case class ArchiveComponent(name: String, data: Array[Byte])

case class ArchiveEntry(name: String, offset: Long, size: Long)

class TantivyArchiveWriter {
  private val logger = LoggerFactory.getLogger(classOf[TantivyArchiveWriter])
  private val components = mutable.ArrayBuffer[ArchiveComponent]()
  
  // Magic number for Tantivy4Spark archives: "TNT4SPRK" in ASCII
  private val MAGIC_NUMBER: Long = 0x544E543453504B52L
  
  def addComponent(name: String, data: Array[Byte]): Unit = {
    logger.debug(s"Adding component '$name' with ${data.length} bytes")
    components += ArchiveComponent(name, data)
  }
  
  def writeArchive(outputStream: OutputStream): Unit = {
    val dataOutput = new DataOutputStream(outputStream)
    val entries = mutable.ArrayBuffer[ArchiveEntry]()
    var currentOffset = 0L
    
    logger.info(s"Writing archive with ${components.size} components")
    
    try {
      // Write component data and track entries
      for (component <- components) {
        val entry = ArchiveEntry(component.name, currentOffset, component.data.length.toLong)
        entries += entry
        
        dataOutput.write(component.data)
        currentOffset += component.data.length
      }
      
      // Write table of contents
      val tocStartOffset = currentOffset
      
      // Number of components
      dataOutput.writeInt(components.size)
      
      // Component entries
      for (entry <- entries) {
        // Component name
        val nameBytes = entry.name.getBytes("UTF-8")
        dataOutput.writeInt(nameBytes.length)
        dataOutput.write(nameBytes)
        
        // Component offset and size
        dataOutput.writeLong(entry.offset)
        dataOutput.writeLong(entry.size)
      }
      
      val tocSize = currentOffset - tocStartOffset + 4 + entries.map { entry =>
        4 + entry.name.getBytes("UTF-8").length + 8 + 8
      }.sum
      
      // Write footer
      dataOutput.writeLong(tocSize)
      dataOutput.writeLong(MAGIC_NUMBER)
      
      logger.info(s"Archive written successfully: ${components.size} components, TOC size: $tocSize bytes")
      
    } finally {
      dataOutput.flush()
    }
  }
}

class TantivyArchiveReader(storage: StorageStrategy) {
  private val logger = LoggerFactory.getLogger(classOf[TantivyArchiveReader])
  private val MAGIC_NUMBER: Long = 0x544E543453504B52L
  
  private lazy val tableOfContents: Map[String, ArchiveEntry] = {
    parseTableOfContents()
  }
  
  def getComponentNames(): Set[String] = {
    tableOfContents.keySet
  }
  
  def hasComponent(name: String): Boolean = {
    tableOfContents.contains(name)
  }
  
  def readComponent(name: String): Array[Byte] = {
    tableOfContents.get(name) match {
      case Some(entry) =>
        logger.debug(s"Reading component '$name' at offset ${entry.offset}, size ${entry.size}")
        storage.readRange(entry.offset, entry.size)
      case None =>
        throw new IllegalArgumentException(s"Component '$name' not found in archive")
    }
  }
  
  def getComponentSize(name: String): Long = {
    tableOfContents.get(name) match {
      case Some(entry) => entry.size
      case None => throw new IllegalArgumentException(s"Component '$name' not found in archive")
    }
  }
  
  private def parseTableOfContents(): Map[String, ArchiveEntry] = {
    val fileSize = storage.getFileSize()
    
    // Read footer (last 16 bytes: TOC size + magic number)
    val footerData = storage.readRange(fileSize - 16, 16)
    val footerBuffer = ByteBuffer.wrap(footerData)
    
    val tocSize = footerBuffer.getLong()
    val magicNumber = footerBuffer.getLong()
    
    if (magicNumber != MAGIC_NUMBER) {
      throw new IllegalArgumentException(s"Invalid archive format: magic number mismatch. Expected: $MAGIC_NUMBER, got: $magicNumber")
    }
    
    logger.debug(s"Archive footer parsed: TOC size = $tocSize, magic number = $magicNumber")
    
    // Read table of contents
    val tocOffset = fileSize - 16 - tocSize
    val tocData = storage.readRange(tocOffset, tocSize)
    val tocBuffer = ByteBuffer.wrap(tocData)
    
    val numComponents = tocBuffer.getInt()
    val entries = mutable.Map[String, ArchiveEntry]()
    
    logger.info(s"Reading table of contents: $numComponents components")
    
    for (_ <- 0 until numComponents) {
      // Read component name
      val nameLength = tocBuffer.getInt()
      val nameBytes = new Array[Byte](nameLength)
      tocBuffer.get(nameBytes)
      val name = new String(nameBytes, "UTF-8")
      
      // Read component offset and size
      val offset = tocBuffer.getLong()
      val size = tocBuffer.getLong()
      
      entries(name) = ArchiveEntry(name, offset, size)
      logger.debug(s"Component '$name': offset=$offset, size=$size")
    }
    
    entries.toMap
  }
}

object TantivyArchiveFormat {
  // Logger removed to eliminate unused warning
  
  /**
   * Creates a new archive from a collection of named components.
   */
  def createArchive(components: Map[String, Array[Byte]], outputStream: OutputStream): Unit = {
    val writer = new TantivyArchiveWriter()
    
    for ((name, data) <- components) {
      writer.addComponent(name, data)
    }
    
    writer.writeArchive(outputStream)
  }
  
  /**
   * Reads an archive and returns all components as a map.
   */
  def readAllComponents(storage: StorageStrategy): Map[String, Array[Byte]] = {
    val reader = new TantivyArchiveReader(storage)
    val components = mutable.Map[String, Array[Byte]]()
    
    for (name <- reader.getComponentNames()) {
      components(name) = reader.readComponent(name)
    }
    
    components.toMap
  }
  
  /**
   * Validates that a file is a valid Tantivy archive.
   */
  def validateArchive(storage: StorageStrategy): Boolean = {
    try {
      val reader = new TantivyArchiveReader(storage)
      // If we can parse the TOC without exception, it's valid
      reader.getComponentNames()
      true
    } catch {
      case _: Exception => false
    }
  }
}