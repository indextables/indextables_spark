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

import org.apache.hadoop.fs.Path
import java.net.URI

object FileProtocolUtils {
  
  def isS3Protocol(path: String): Boolean = {
    val uri = new URI(path)
    val scheme = Option(uri.getScheme).getOrElse("file")
    scheme.toLowerCase match {
      case "s3" | "s3a" | "s3n" => true
      case _ => false
    }
  }
  
  def shouldUseS3OptimizedIO(path: String, options: Map[String, String]): Boolean = {
    val forceStandard = options.getOrElse("spark.tantivy.storage.force.standard", "false").toBoolean
    isS3Protocol(path) && !forceStandard
  }
  
  def shouldUseS3OptimizedIO(path: Path, options: Map[String, String]): Boolean = {
    shouldUseS3OptimizedIO(path.toString, options)
  }
  
  def isS3Protocol(path: Path): Boolean = {
    isS3Protocol(path.toString)
  }
  
  def getProtocol(path: String): String = {
    val uri = new URI(path)
    Option(uri.getScheme).getOrElse("file")
  }
  
  def getProtocol(path: Path): String = {
    getProtocol(path.toString)
  }
  
  def parseS3Location(path: String): Option[S3Location] = {
    if (!isS3Protocol(path)) {
      return None
    }
    
    try {
      val uri = new URI(path)
      val bucket = uri.getHost
      val key = if (uri.getPath.startsWith("/")) {
        uri.getPath.substring(1)
      } else {
        uri.getPath
      }
      Some(S3Location(bucket, key))
    } catch {
      case _: Exception => None
    }
  }
}

case class S3Location(bucket: String, key: String)