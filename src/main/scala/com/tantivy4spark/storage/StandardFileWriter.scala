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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path, FSDataOutputStream}
import java.io.IOException
import scala.util.{Try, Success, Failure}

class StandardFileWriter(
    hadoopConf: Configuration,
    options: Map[String, String]
) {
  
  def writeToFile(filePath: String, data: Array[Byte], append: Boolean = false): Try[Long] = {
    Try {
      val path = new Path(filePath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      
      // Ensure parent directories exist
      val parentPath = path.getParent
      if (parentPath != null && !fs.exists(parentPath)) {
        fs.mkdirs(parentPath)
      }
      
      val outputStream = if (append && fs.exists(path)) {
        fs.append(path)
      } else {
        fs.create(path, true) // overwrite if exists
      }
      
      try {
        outputStream.write(data)
        outputStream.flush()
        data.length.toLong
      } finally {
        outputStream.close()
      }
    }
  }
  
  def createOutputStream(filePath: String, overwrite: Boolean = true): Try[FSDataOutputStream] = {
    Try {
      val path = new Path(filePath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      
      // Ensure parent directories exist
      val parentPath = path.getParent
      if (parentPath != null && !fs.exists(parentPath)) {
        fs.mkdirs(parentPath)
      }
      
      fs.create(path, overwrite)
    }
  }
  
  def deleteFile(filePath: String): Boolean = {
    Try {
      val path = new Path(filePath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      if (fs.exists(path)) {
        fs.delete(path, false)
      } else {
        false
      }
    }.getOrElse(false)
  }
  
  def exists(filePath: String): Boolean = {
    Try {
      val path = new Path(filePath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      fs.exists(path)
    }.getOrElse(false)
  }
  
  def mkdirs(dirPath: String): Boolean = {
    Try {
      val path = new Path(dirPath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      fs.mkdirs(path)
    }.getOrElse(false)
  }
  
  def listFiles(dirPath: String): List[String] = {
    Try {
      val path = new Path(dirPath)
      val fs = FileSystem.get(path.toUri, hadoopConf)
      if (fs.exists(path) && fs.getFileStatus(path).isDirectory) {
        fs.listStatus(path).map(_.getPath.toString).toList
      } else {
        List.empty
      }
    }.getOrElse(List.empty)
  }
}