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

import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.fs.Path

class FileProtocolUtilsTest extends AnyFunSuite {
  
  test("should identify S3 protocols correctly") {
    assert(FileProtocolUtils.isS3Protocol("s3://bucket/key"))
    assert(FileProtocolUtils.isS3Protocol("s3a://bucket/key"))
    assert(FileProtocolUtils.isS3Protocol("s3n://bucket/key"))
    assert(!FileProtocolUtils.isS3Protocol("hdfs://namenode/path"))
    assert(!FileProtocolUtils.isS3Protocol("file:///local/path"))
    assert(!FileProtocolUtils.isS3Protocol("/absolute/path"))
    assert(!FileProtocolUtils.isS3Protocol("relative/path"))
  }
  
  test("should identify S3 protocols correctly for Path objects") {
    assert(FileProtocolUtils.isS3Protocol(new Path("s3://bucket/key")))
    assert(FileProtocolUtils.isS3Protocol(new Path("s3a://bucket/key")))
    assert(!FileProtocolUtils.isS3Protocol(new Path("hdfs://namenode/path")))
    assert(!FileProtocolUtils.isS3Protocol(new Path("file:///local/path")))
  }
  
  test("should extract protocol correctly") {
    assert(FileProtocolUtils.getProtocol("s3://bucket/key") == "s3")
    assert(FileProtocolUtils.getProtocol("s3a://bucket/key") == "s3a")
    assert(FileProtocolUtils.getProtocol("hdfs://namenode/path") == "hdfs")
    assert(FileProtocolUtils.getProtocol("file:///local/path") == "file")
    assert(FileProtocolUtils.getProtocol("/absolute/path") == "file")
    assert(FileProtocolUtils.getProtocol("relative/path") == "file")
  }
  
  test("should respect force standard config for S3") {
    val s3Path = "s3://bucket/key"
    val optionsWithForceStandard = Map("spark.tantivy.storage.force.standard" -> "true")
    val optionsWithoutForce = Map[String, String]()
    
    assert(!FileProtocolUtils.shouldUseS3OptimizedIO(s3Path, optionsWithForceStandard))
    assert(FileProtocolUtils.shouldUseS3OptimizedIO(s3Path, optionsWithoutForce))
  }
  
  test("should not use S3 optimization for non-S3 protocols") {
    val hdfsPath = "hdfs://namenode/path"
    val localPath = "file:///local/path"
    val options = Map[String, String]()
    
    assert(!FileProtocolUtils.shouldUseS3OptimizedIO(hdfsPath, options))
    assert(!FileProtocolUtils.shouldUseS3OptimizedIO(localPath, options))
  }
  
  test("should parse S3 location correctly") {
    val s3Location = FileProtocolUtils.parseS3Location("s3://my-bucket/path/to/file.txt")
    assert(s3Location.isDefined)
    assert(s3Location.get.bucket == "my-bucket")
    assert(s3Location.get.key == "path/to/file.txt")
    
    val s3aLocation = FileProtocolUtils.parseS3Location("s3a://another-bucket/key")
    assert(s3aLocation.isDefined)
    assert(s3aLocation.get.bucket == "another-bucket")
    assert(s3aLocation.get.key == "key")
    
    val nonS3Location = FileProtocolUtils.parseS3Location("hdfs://namenode/path")
    assert(nonS3Location.isEmpty)
  }
  
  test("should handle edge cases in S3 location parsing") {
    val rootLocation = FileProtocolUtils.parseS3Location("s3://bucket/")
    assert(rootLocation.isDefined)
    assert(rootLocation.get.bucket == "bucket")
    assert(rootLocation.get.key == "")
    
    val invalidLocation = FileProtocolUtils.parseS3Location("not-a-valid-uri")
    assert(invalidLocation.isEmpty)
  }
}