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


package com.tantivy4spark.core

import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, TableCapability}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.sources.{BaseRelation, CreatableRelationProvider, DataSourceRegister, RelationProvider, TableScan}
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import com.tantivy4spark.transaction.TransactionLog
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.SparkSession

import java.util
import scala.collection.JavaConverters._

class Tantivy4SparkDataSource extends DataSourceRegister with RelationProvider with CreatableRelationProvider {
  override def shortName(): String = "tantivy4spark"

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]
  ): BaseRelation = {
    // For reads, create a relation that can handle queries
    val path = parameters.getOrElse("path", throw new IllegalArgumentException("Path is required"))
    new Tantivy4SparkRelation(path, sqlContext)
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame
  ): BaseRelation = {
    // For writes, delegate to the V2 TableProvider approach
    val path = parameters.getOrElse("path", throw new IllegalArgumentException("Path is required"))
    
    // Use the V2 API by creating the table and executing the write
    val spark = sqlContext.sparkSession
    val options = new CaseInsensitiveStringMap(parameters.asJava)
    val tableProvider = new Tantivy4SparkTableProvider()
    
    // Get or create the table
    val table = tableProvider.getTable(data.schema, Array.empty, options)
    
    // Create write info
    val writeInfo = new LogicalWriteInfo {
      override def queryId(): String = java.util.UUID.randomUUID().toString
      override def schema(): StructType = data.schema
      override def options(): CaseInsensitiveStringMap = options
    }
    
    // Get the write builder and execute the write
    val writeBuilder = table.asInstanceOf[Tantivy4SparkTable].newWriteBuilder(writeInfo)
    val write = writeBuilder.build()
    val batchWrite = write.asInstanceOf[com.tantivy4spark.core.Tantivy4SparkBatchWrite]
    
    // Extract serializable parameters before the closure
    val serializableOptions = parameters
    val serializablePath = path
    val serializableSchema = data.schema
    
    // Extract essential Hadoop configuration properties as a Map
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    val essentialConfProps = Map(
      "fs.defaultFS" -> hadoopConf.get("fs.defaultFS", ""),
      "fs.s3a.access.key" -> hadoopConf.get("fs.s3a.access.key", ""),
      "fs.s3a.secret.key" -> hadoopConf.get("fs.s3a.secret.key", ""),
      "fs.s3a.endpoint" -> hadoopConf.get("fs.s3a.endpoint", ""),
      "fs.s3a.path.style.access" -> hadoopConf.get("fs.s3a.path.style.access", ""),
      "fs.s3a.impl" -> hadoopConf.get("fs.s3a.impl", ""),
      "fs.hdfs.impl" -> hadoopConf.get("fs.hdfs.impl", ""),
      "fs.file.impl" -> hadoopConf.get("fs.file.impl", "")
    ).filter(_._2.nonEmpty)
    
    // Execute write using Spark's mapPartitionsWithIndex
    val commitMessages = data.queryExecution.toRdd.mapPartitionsWithIndex { (partitionId, iterator) =>
      // Recreate Hadoop configuration with essential properties in the executor
      val localHadoopConf = new org.apache.hadoop.conf.Configuration()
      essentialConfProps.foreach { case (key, value) =>
        localHadoopConf.set(key, value)
      }
      
      val localWriterFactory = new com.tantivy4spark.core.Tantivy4SparkWriterFactory(
        new Path(serializablePath),
        serializableSchema,
        new CaseInsensitiveStringMap(serializableOptions.asJava),
        localHadoopConf
      )
      val writer = localWriterFactory.createWriter(partitionId, 0L)
      try {
        iterator.foreach(row => writer.write(row))
        Seq(writer.commit()).iterator
      } catch {
        case e: Exception =>
          writer.abort()
          throw e
      } finally {
        writer.close()
      }
    }.collect()
    
    // Commit all the writes
    batchWrite.commit(commitMessages)
    
    // Return a relation for reading the written data
    createRelation(sqlContext, parameters)
  }
}

class Tantivy4SparkRelation(
    path: String,
    val sqlContext: SQLContext
) extends BaseRelation with TableScan {
  
  override def schema: StructType = {
    // Get schema from transaction log
    val spark = sqlContext.sparkSession
    val transactionLog = new TransactionLog(new Path(path), spark)
    transactionLog.getSchema().getOrElse {
      throw new IllegalArgumentException(s"Unable to infer schema from path: $path")
    }
  }
  
  override def buildScan(): RDD[org.apache.spark.sql.Row] = {
    val spark = sqlContext.sparkSession
    val transactionLog = new TransactionLog(new Path(path), spark)
    
    // Get list of files from transaction log
    val files = transactionLog.listFiles()
    
    if (files.isEmpty) {
      // Return empty RDD if no files
      spark.sparkContext.emptyRDD[org.apache.spark.sql.Row]
    } else {
      // Extract serializable data
      val serializableFiles = files.map(_.path)
      val serializableSchema = schema
      
      // Get Hadoop configuration from driver context
      val hadoopConfProps = Map(
        "fs.defaultFS" -> spark.sparkContext.hadoopConfiguration.get("fs.defaultFS", ""),
        "fs.s3a.access.key" -> spark.sparkContext.hadoopConfiguration.get("fs.s3a.access.key", ""),
        "fs.s3a.secret.key" -> spark.sparkContext.hadoopConfiguration.get("fs.s3a.secret.key", ""),
        "fs.s3a.endpoint" -> spark.sparkContext.hadoopConfiguration.get("fs.s3a.endpoint", ""),
        "fs.s3a.path.style.access" -> spark.sparkContext.hadoopConfiguration.get("fs.s3a.path.style.access", ""),
        "fs.s3a.impl" -> spark.sparkContext.hadoopConfiguration.get("fs.s3a.impl", ""),
        "fs.hdfs.impl" -> spark.sparkContext.hadoopConfiguration.get("fs.hdfs.impl", ""),
        "fs.file.impl" -> spark.sparkContext.hadoopConfiguration.get("fs.file.impl", "")
      ).filter(_._2.nonEmpty)
      
      // Create RDD from file paths
      spark.sparkContext.parallelize(serializableFiles).flatMap { filePath =>
        // Recreate Hadoop configuration in executor context
        val localHadoopConf = new org.apache.hadoop.conf.Configuration()
        hadoopConfProps.foreach { case (key, value) =>
          localHadoopConf.set(key, value)
        }
        
        // Create storage reader for the Tantivy archive file
        val reader = new com.tantivy4spark.storage.StandardFileReader(new Path(filePath), localHadoopConf)
        val rows = scala.collection.mutable.ListBuffer[org.apache.spark.sql.Row]()
        
        try {
          // Read the Tantivy archive and extract index components
          println(s"Reading Tantivy archive: $filePath")
          val indexComponents = com.tantivy4spark.storage.TantivyArchiveFormat.readAllComponents(reader)
          println(s"Read ${indexComponents.size} components: ${indexComponents.keys.mkString(", ")}")
          
          // Create Tantivy search engine from the components
          val searchEngine = com.tantivy4spark.search.TantivySearchEngine.fromIndexComponents(serializableSchema, indexComponents)
          
          // For now, use a match-all search to get all documents
          // TODO: Implement proper filter pushdown for specific queries
          println(s"Using search engine with schema: ${serializableSchema.fieldNames.mkString(", ")}")
          val results = searchEngine.searchAll(limit = 10000)
          println(s"Search returned ${results.length} results")
          
          // Convert search results to Spark Rows with proper type conversion
          results.foreach { internalRow =>
            try {
              val row = org.apache.spark.sql.catalyst.CatalystTypeConverters.convertToScala(internalRow, serializableSchema).asInstanceOf[org.apache.spark.sql.Row]
              rows += row
            } catch {
              case ex: Exception =>
                // If catalyst conversion fails, manually convert the row
                println(s"Catalyst conversion failed, using manual conversion: ${ex.getMessage}")
                val values = serializableSchema.fields.zipWithIndex.map { case (field, idx) =>
                  val rawValue = if (idx < internalRow.numFields && !internalRow.isNullAt(idx)) {
                    field.dataType match {
                      case org.apache.spark.sql.types.LongType => internalRow.getLong(idx)
                      case org.apache.spark.sql.types.IntegerType => internalRow.getInt(idx)
                      case org.apache.spark.sql.types.DoubleType => internalRow.getDouble(idx)
                      case org.apache.spark.sql.types.BooleanType => internalRow.getBoolean(idx)
                      case org.apache.spark.sql.types.StringType => internalRow.getUTF8String(idx).toString
                      case _: org.apache.spark.sql.types.TimestampType => 
                        val longVal = try {
                          internalRow.getLong(idx)
                        } catch {
                          case _: ClassCastException => internalRow.getInt(idx).toLong
                        }
                        new java.sql.Timestamp(longVal)
                      case _: org.apache.spark.sql.types.DateType => 
                        val longVal = try {
                          internalRow.getLong(idx)
                        } catch {
                          case _: ClassCastException => internalRow.getInt(idx).toLong
                        }
                        new java.sql.Date(longVal)
                      case _ => internalRow.get(idx, field.dataType)
                    }
                  } else {
                    null
                  }
                  rawValue
                }
                rows += org.apache.spark.sql.Row(values: _*)
            }
          }
          
          searchEngine.close()
          println(s"Converted ${rows.length} rows from search")
        } catch {
          case ex: Exception =>
            // If we can't read the Tantivy archive, log and return empty
            System.err.println(s"Failed to read Tantivy archive $filePath: ${ex.getMessage}")
            ex.printStackTrace()
            // Return empty iterator on error
          } finally {
            reader.close()
          }
        
        rows.toIterator
      }
    }
  }
}

class Tantivy4SparkTable(
    path: String,
    schema: StructType,
    options: CaseInsensitiveStringMap
) extends SupportsRead with SupportsWrite {

  private val spark = SparkSession.active
  private val tablePath = new Path(path)
  private val transactionLog = new TransactionLog(tablePath, spark)

  override def name(): String = s"tantivy4spark.`$path`"

  override def schema(): StructType = {
    transactionLog.getSchema().getOrElse(schema)
  }

  override def capabilities(): util.Set[TableCapability] = {
    Set(
      TableCapability.BATCH_READ,
      TableCapability.BATCH_WRITE,
      TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE
    ).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = {
    new Tantivy4SparkScanBuilder(transactionLog, schema(), options)
  }

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val hadoopConf = spark.sparkContext.hadoopConfiguration
    new Tantivy4SparkWriteBuilder(transactionLog, tablePath, info, options, hadoopConf)
  }
}

class Tantivy4SparkTableProvider extends org.apache.spark.sql.connector.catalog.TableProvider {

  /**
   * Extracts paths from options, supporting both direct path parameters and multiple paths.
   * Handles paths from load("path") calls as well as .option("path", "value") and multiple paths.
   */
  protected def getPaths(options: CaseInsensitiveStringMap): Seq[String] = {
    val paths = Option(options.get("paths")).map { pathStr =>
      parsePathsFromJson(pathStr)
    }.getOrElse(Seq.empty)
    
    paths ++ Option(options.get("path")).toSeq
  }
  
  private def parsePathsFromJson(pathStr: String): Seq[String] = {
    try {
      import com.fasterxml.jackson.databind.ObjectMapper
      import com.fasterxml.jackson.module.scala.DefaultScalaModule
      
      val objectMapper = new ObjectMapper()
      objectMapper.registerModule(DefaultScalaModule)
      objectMapper.readValue(pathStr, classOf[Array[String]]).toSeq
    } catch {
      case _: Exception => 
        throw new IllegalArgumentException(s"Invalid paths format: $pathStr. Expected JSON array of strings.")
    }
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = {
    val paths = getPaths(options)
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    val spark = SparkSession.active
    val transactionLog = new TransactionLog(new Path(paths.head), spark)
    
    transactionLog.getSchema().getOrElse {
      throw new IllegalArgumentException(s"Unable to infer schema from path: ${paths.head}")
    }
  }

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]
  ): org.apache.spark.sql.connector.catalog.Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val paths = getPaths(options)
    
    if (paths.isEmpty) {
      throw new IllegalArgumentException(
        "Path is required. Use load(\"path\") or .option(\"path\", \"value\").load()"
      )
    }

    // Use the first path as the primary table path (support for multiple paths can be added later)
    new Tantivy4SparkTable(paths.head, schema, options)
  }

  override def supportsExternalMetadata(): Boolean = true
}