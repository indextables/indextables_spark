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

/** Shared timing measurement utility. */
object TimingUtils {

  /**
   * Execute a block and return its result along with the elapsed time in milliseconds.
   *
   * @param block
   *   The code block to time
   * @return
   *   Tuple of (result, durationMs)
   */
  def timed[T](block: => T): (T, Long) = {
    val start  = System.currentTimeMillis()
    val result = block
    (result, System.currentTimeMillis() - start)
  }
}
