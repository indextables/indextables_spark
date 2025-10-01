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
import org.apache.hadoop.fs.Path
import java.io.Closeable

trait StorageStrategy extends Closeable {
  def readFile(): Array[Byte]
  def readRange(offset: Long, length: Long): Array[Byte]
  def getFileSize(): Long
}

object StorageStrategyFactory {

  def createReader(path: Path, conf: Configuration): StorageStrategy = {
    val protocol      = path.toUri.getScheme
    val forceStandard = conf.getBoolean("spark.indextables.storage.force.standard", false)

    if (!forceStandard && (protocol == "s3" || protocol == "s3a" || protocol == "s3n")) {
      new S3OptimizedReader(path, conf)
    } else {
      new StandardFileReader(path, conf)
    }
  }
}
