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

import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class CloudPathUtilsTest extends AnyFunSuite with Matchers {

  test("stripFileScheme converts file:///path to /path") {
    CloudPathUtils.stripFileScheme("file:///var/data/file.txt") shouldBe "/var/data/file.txt"
  }

  test("stripFileScheme converts file:/path to /path") {
    CloudPathUtils.stripFileScheme("file:/var/data/file.txt") shouldBe "/var/data/file.txt"
  }

  test("stripFileScheme converts file://localhost/path to /path") {
    CloudPathUtils.stripFileScheme("file://localhost/var/data/file.txt") shouldBe "/var/data/file.txt"
  }

  test("stripFileScheme handles file:/// root") {
    CloudPathUtils.stripFileScheme("file:///") shouldBe "/"
  }

  test("stripFileScheme returns non-file paths unchanged") {
    CloudPathUtils.stripFileScheme("/var/data/file.txt") shouldBe "/var/data/file.txt"
    CloudPathUtils.stripFileScheme("s3://bucket/key") shouldBe "s3://bucket/key"
    CloudPathUtils.stripFileScheme("abfss://container@account/path") shouldBe "abfss://container@account/path"
  }

  test("stripFileScheme returns empty string unchanged") {
    CloudPathUtils.stripFileScheme("") shouldBe ""
  }

  test("stripFileScheme falls back to substring stripping for malformed URIs") {
    CloudPathUtils.stripFileScheme("file:///path with spaces/file.txt") shouldBe "/path with spaces/file.txt"
  }

  test("stripFileScheme decodes percent-encoded characters") {
    CloudPathUtils.stripFileScheme("file:///path%20with%20spaces/file.txt") shouldBe "/path with spaces/file.txt"
  }
}
