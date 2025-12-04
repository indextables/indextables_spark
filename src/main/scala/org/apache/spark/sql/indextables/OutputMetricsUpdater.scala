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

package org.apache.spark.sql.indextables

import org.apache.spark.TaskContext

/**
 * Helper object to update Spark's task metrics from external packages.
 *
 * Since InputMetrics/OutputMetrics setter methods are private[spark], this helper class in the org.apache.spark package
 * provides access to them.
 *
 * This allows IndexTables4Spark to report bytes/records read and written to the Spark UI, similar to how native Spark
 * data sources report metrics.
 */
object OutputMetricsUpdater {

  /**
   * Update the output metrics for the current task.
   *
   * @param bytesWritten
   *   Total bytes written by this task
   * @param recordsWritten
   *   Total records written by this task
   * @return
   *   true if metrics were updated, false if TaskContext is not available
   */
  def updateOutputMetrics(bytesWritten: Long, recordsWritten: Long): Boolean =
    Option(TaskContext.get())
      .map { taskContext =>
        val outputMetrics = taskContext.taskMetrics().outputMetrics
        outputMetrics.setBytesWritten(bytesWritten)
        outputMetrics.setRecordsWritten(recordsWritten)
        true
      }
      .getOrElse(false)

  /**
   * Update the input metrics for the current task.
   *
   * @param bytesRead
   *   Total bytes read by this task
   * @param recordsRead
   *   Total records read by this task
   * @return
   *   true if metrics were updated, false if TaskContext is not available
   */
  def updateInputMetrics(bytesRead: Long, recordsRead: Long): Boolean =
    Option(TaskContext.get())
      .map { taskContext =>
        val inputMetrics = taskContext.taskMetrics().inputMetrics
        inputMetrics.setBytesRead(bytesRead)
        inputMetrics.setRecordsRead(recordsRead)
        true
      }
      .getOrElse(false)

  /**
   * Increment the input metrics for the current task. Use this for incremental updates during iteration.
   *
   * @param bytesRead
   *   Additional bytes read
   * @param recordsRead
   *   Additional records read
   * @return
   *   true if metrics were updated, false if TaskContext is not available
   */
  def incInputMetrics(bytesRead: Long, recordsRead: Long): Boolean =
    Option(TaskContext.get())
      .map { taskContext =>
        val inputMetrics = taskContext.taskMetrics().inputMetrics
        inputMetrics.incBytesRead(bytesRead)
        inputMetrics.incRecordsRead(recordsRead)
        true
      }
      .getOrElse(false)
}
