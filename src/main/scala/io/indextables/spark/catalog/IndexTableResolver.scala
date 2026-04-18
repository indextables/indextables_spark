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

package io.indextables.spark.catalog

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}

import org.slf4j.LoggerFactory

/**
 * Utility for resolving an IndexTables companion index path from source table TBLPROPERTIES.
 *
 * Reusable by the catalog and, in the future, by SQL commands (MERGE SPLITS AT TABLE ..., etc.)
 * when they add support for table-name references instead of raw storage paths.
 */
object IndexTableResolver {

  private val logger = LoggerFactory.getLogger(getClass)

  val PROP_INDEX_ROOT_PREFIX  = "indextables.companion.indexroot"
  val PROP_RELATIVE_PATH      = "indextables.companion.tableroot.relativepath"

  /**
   * Detects the current cluster region from Spark/Hadoop configuration and environment variables.
   *
   * Resolution order (mirrors CloudStorageProvider.scala region resolution chain):
   *   1. spark.indextables.aws.region  (Spark session conf)
   *   2. fs.s3a.endpoint.region        (Hadoop conf)
   *   3. aws.region                    (JVM system property)
   *   4. AWS_DEFAULT_REGION            (environment variable)
   *   5. AWS_REGION                    (environment variable)
   */
  def detectRegion(spark: SparkSession): Option[String] =
    spark.conf.getOption("spark.indextables.aws.region")
      .orElse(Option(spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint.region")))
      .orElse(Option(System.getProperty("aws.region")))
      .orElse(sys.env.get("AWS_DEFAULT_REGION"))
      .orElse(sys.env.get("AWS_REGION"))

  /**
   * Resolves the index path from the given source table properties.
   *
   * Reads:
   *   - indextables.companion.indexroot.{region}  — region-specific storage root (preferred)
   *   - indextables.companion.indexroot            — fallback storage root
   *   - indextables.companion.tableroot.relativepath — relative path within the index root
   *
   * @param sourceTableProperties properties map from the source table (Delta/Iceberg/Parquet)
   * @param spark                 active SparkSession (for region auto-detection)
   * @param sourceTableLabel      human-readable label for the source table, used in error messages
   * @return resolved absolute path to the companion index
   * @throws IllegalArgumentException if required properties are absent or empty
   */
  def resolveIndexPath(
    sourceTableProperties: java.util.Map[String, String],
    spark: SparkSession,
    sourceTableLabel: String
  ): String = {
    val detectedRegion = detectRegion(spark)
    logger.debug(s"Resolving index path for '$sourceTableLabel'; detected region: $detectedRegion")

    val indexRoot: String = detectedRegion.flatMap { region =>
      val regionKey = s"$PROP_INDEX_ROOT_PREFIX.$region"
      Option(sourceTableProperties.get(regionKey)).filter(_.nonEmpty).map { v =>
        logger.debug(s"Using region-specific index root '$regionKey' = '$v'")
        v
      }
    }.orElse {
      Option(sourceTableProperties.get(PROP_INDEX_ROOT_PREFIX)).filter(_.nonEmpty).map { v =>
        logger.debug(s"Using fallback index root '$PROP_INDEX_ROOT_PREFIX' = '$v'")
        v
      }
    }.getOrElse {
      val regionKeys = detectedRegion.map(r => s"\n  - '$PROP_INDEX_ROOT_PREFIX.$r' (region-specific)").getOrElse("")
      throw new IllegalArgumentException(
        s"Cannot resolve index path for '$sourceTableLabel': " +
        s"no index root property found.$regionKeys\n" +
        s"  - '$PROP_INDEX_ROOT_PREFIX' (fallback)\n\n" +
        s"Set via ALTER TABLE on the source table:\n" +
        s"  ALTER TABLE $sourceTableLabel SET TBLPROPERTIES (\n" +
        s"    '$PROP_INDEX_ROOT_PREFIX' = 's3://my-index-bucket/indexes'\n" +
        s"  );"
      )
    }

    val relativePath = Option(sourceTableProperties.get(PROP_RELATIVE_PATH)).filter(_.nonEmpty).getOrElse {
      throw new IllegalArgumentException(
        s"Cannot resolve index path for '$sourceTableLabel': " +
        s"property '$PROP_RELATIVE_PATH' is missing or empty.\n\n" +
        s"Set via ALTER TABLE on the source table:\n" +
        s"  ALTER TABLE $sourceTableLabel SET TBLPROPERTIES (\n" +
        s"    '$PROP_RELATIVE_PATH' = 'company/my_table'\n" +
        s"  );"
      )
    }

    val resolved = s"${indexRoot.stripSuffix("/")}/${relativePath.stripPrefix("/")}"
    logger.info(s"Resolved index path for '$sourceTableLabel': $resolved")
    resolved
  }

  /**
   * Loads a source table via CatalogManager and resolves its companion index path.
   *
   * @param sourceCatalogName  name of the source catalog (e.g. "unity_catalog")
   * @param sourceNamespace    namespace segments within the source catalog (e.g. Array("production"))
   * @param sourceTableName    table name within the namespace (e.g. "users")
   * @param spark              active SparkSession
   * @return resolved absolute path to the companion index
   * @throws IllegalArgumentException      if required TBLPROPERTIES are missing
   * @throws UnsupportedOperationException if the source catalog does not implement TableCatalog
   */
  def resolveFromCatalog(
    sourceCatalogName: String,
    sourceNamespace: Array[String],
    sourceTableName: String,
    spark: SparkSession
  ): String = {
    val sourceLabel = (Seq(sourceCatalogName) ++ sourceNamespace ++ Seq(sourceTableName)).mkString(".")

    val catalogPlugin = spark.sessionState.catalogManager.catalog(sourceCatalogName)

    val sourceCatalog = catalogPlugin match {
      case tc: TableCatalog => tc
      case other =>
        throw new UnsupportedOperationException(
          s"Source catalog '$sourceCatalogName' (${other.getClass.getName}) does not implement " +
          s"TableCatalog. Only V2 catalogs (Delta, Iceberg, etc.) are supported for companion " +
          s"index resolution. Check your spark.sql.catalog.$sourceCatalogName configuration."
        )
    }

    val sourceIdent  = Identifier.of(sourceNamespace, sourceTableName)
    val sourceTable  = sourceCatalog.loadTable(sourceIdent)   // throws NoSuchTableException if absent
    val props        = sourceTable.properties()

    resolveIndexPath(props, spark, sourceLabel)
  }
}
