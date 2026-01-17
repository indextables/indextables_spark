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

package io.indextables.spark.sql

import scala.jdk.CollectionConverters._

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference}
import org.apache.spark.sql.execution.command.LeafRunnableCommand
import org.apache.spark.sql.types._

import io.indextables.spark.util.CredentialRedaction
import org.slf4j.LoggerFactory

/**
 * SQL command to describe environment configuration across all executors.
 *
 * Syntax: DESCRIBE INDEXTABLES ENVIRONMENT DESCRIBE TANTIVY4SPARK ENVIRONMENT
 *
 * Returns a DataFrame with Spark and Hadoop configuration properties from driver and all workers:
 *   - host: Host address of the executor
 *   - role: "driver" or "worker"
 *   - property_type: "spark" or "hadoop"
 *   - property_name: Configuration property name
 *   - property_value: Configuration property value (sensitive values are redacted)
 *
 * Note: Sensitive configuration values (containing "secret", "key", "password", "token", "credential", or "session")
 * are automatically redacted for security.
 */
case class DescribeEnvironmentCommand() extends LeafRunnableCommand {

  private val logger = LoggerFactory.getLogger(classOf[DescribeEnvironmentCommand])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("host", StringType, nullable = false)(),
    AttributeReference("role", StringType, nullable = false)(),
    AttributeReference("property_type", StringType, nullable = false)(),
    AttributeReference("property_name", StringType, nullable = false)(),
    AttributeReference("property_value", StringType, nullable = true)()
  )

  override def run(sparkSession: SparkSession): Seq[Row] = {
    logger.info("Describing environment configuration across all executors")

    val sc = sparkSession.sparkContext

    // Get the driver's block manager to identify it
    val driverBlockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
    val driverHostPort       = s"${driverBlockManagerId.host}:${driverBlockManagerId.port}"

    // Count only actual executors (exclude driver), at least 1 for local mode
    val numExecutors = math.max(1, sc.getExecutorMemoryStatus.keys.count(_ != driverHostPort))

    // Collect Spark configuration from the driver (available on driver only)
    val sparkConf = sparkSession.conf.getAll

    // Collect Hadoop configuration from the driver
    val hadoopConf = sparkSession.sparkContext.hadoopConfiguration

    try {
      // Collect configuration from all executors
      val executorRows = sc
        .parallelize(1 to numExecutors, numExecutors)
        .mapPartitionsWithIndex { (index, _) =>
          val blockManagerId = org.apache.spark.SparkEnv.get.blockManager.blockManagerId
          val host           = s"${blockManagerId.host}:${blockManagerId.port}"

          // On executors, we can only access SparkEnv conf, not SparkSession
          // Get Spark properties from SparkEnv
          val sparkEnv   = org.apache.spark.SparkEnv.get
          val sparkProps = sparkEnv.conf.getAll

          // Get Hadoop configuration
          val hadoopProps = {
            val conf = new org.apache.hadoop.conf.Configuration()
            conf.iterator().asScala.map(entry => entry.getKey -> entry.getValue).toSeq
          }

          // Build rows for Spark properties
          val sparkRows = sparkProps.map {
            case (name, value) =>
              Row(
                host,
                "worker",
                "spark",
                name,
                CredentialRedaction.redactValue(name, value)
              )
          }

          // Build rows for Hadoop properties
          val hadoopRows = hadoopProps.map {
            case (name, value) =>
              Row(
                host,
                "worker",
                "hadoop",
                name,
                CredentialRedaction.redactValue(name, value)
              )
          }

          (sparkRows ++ hadoopRows).iterator
        }
        .collect()
        .toSeq

      // Build driver rows for Spark properties
      val driverSparkRows = sparkConf.map {
        case (name, value) =>
          Row(
            driverHostPort,
            "driver",
            "spark",
            name,
            CredentialRedaction.redactValue(name, value)
          )
      }.toSeq

      // Build driver rows for Hadoop properties
      val driverHadoopRows = hadoopConf
        .iterator()
        .asScala
        .map { entry =>
          Row(
            driverHostPort,
            "driver",
            "hadoop",
            entry.getKey,
            CredentialRedaction.redactValue(entry.getKey, entry.getValue)
          )
        }
        .toSeq

      // Return driver rows first, then executor rows
      driverSparkRows ++ driverHadoopRows ++ executorRows

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to collect executor configuration, returning driver-only: ${e.getMessage}")
        // Fallback to driver-only configuration
        val driverSparkRows = sparkConf.map {
          case (name, value) =>
            Row(
              driverHostPort,
              "driver",
              "spark",
              name,
              CredentialRedaction.redactValue(name, value)
            )
        }.toSeq

        val driverHadoopRows = hadoopConf
          .iterator()
          .asScala
          .map { entry =>
            Row(
              driverHostPort,
              "driver",
              "hadoop",
              entry.getKey,
              CredentialRedaction.redactValue(entry.getKey, entry.getValue)
            )
          }
          .toSeq

        driverSparkRows ++ driverHadoopRows
    }
  }
}
