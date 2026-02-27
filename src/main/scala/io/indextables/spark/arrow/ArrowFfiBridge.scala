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

package io.indextables.spark.arrow

import org.apache.arrow.c.{ArrowArray, ArrowSchema, CDataDictionaryProvider, Data}
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.FieldVector
import org.apache.spark.sql.vectorized.{ArrowColumnVector, ColumnarBatch, ColumnVector}
import org.slf4j.LoggerFactory

/**
 * Bridge between tantivy4java's Arrow FFI export and Spark's ColumnarBatch.
 *
 * Manages FFI struct allocation and Arrow import. One instance per partition reader, reused across batches. Call
 * close() when done to release the dictionary provider.
 */
class ArrowFfiBridge extends AutoCloseable {

  private val logger             = LoggerFactory.getLogger(classOf[ArrowFfiBridge])
  private val allocator          = ArrowFfiBridge.allocator
  // NOTE: dictionaryProvider accumulates state across importAsColumnarBatch calls.
  // Safe for single-batch-per-reader model but would need per-batch cleanup for multi-batch streaming.
  private val dictionaryProvider = new CDataDictionaryProvider()

  /**
   * Allocate Arrow C structs for the given number of columns.
   *
   * @param numCols
   *   number of columns to allocate structs for
   * @return
   *   (ArrowArray[], ArrowSchema[], arrayAddresses, schemaAddresses)
   */
  def allocateStructs(numCols: Int): (Array[ArrowArray], Array[ArrowSchema], Array[Long], Array[Long]) = {
    val arrays      = (0 until numCols).map(_ => ArrowArray.allocateNew(allocator)).toArray
    val schemas     = (0 until numCols).map(_ => ArrowSchema.allocateNew(allocator)).toArray
    val arrayAddrs  = arrays.map(_.memoryAddress())
    val schemaAddrs = schemas.map(_.memoryAddress())
    logger.debug(s"Allocated $numCols Arrow FFI struct pairs")
    (arrays, schemas, arrayAddrs, schemaAddrs)
  }

  /**
   * Import filled Arrow C structs into a Spark ColumnarBatch.
   *
   * After this call, the ArrowArray/ArrowSchema structs have been consumed (ownership transferred to the
   * FieldVectors). The returned ColumnarBatch owns the native memory — call batch.close() when done.
   *
   * @param arrays
   *   ArrowArray structs filled by native FFI call
   * @param schemas
   *   ArrowSchema structs filled by native FFI call
   * @param numRows
   *   number of rows in the batch
   * @return
   *   Spark ColumnarBatch wrapping the Arrow data (zero-copy)
   */
  def importAsColumnarBatch(
    arrays: Array[ArrowArray],
    schemas: Array[ArrowSchema],
    numRows: Int
  ): ColumnarBatch = {
    val importedVectors = new scala.collection.mutable.ArrayBuffer[ArrowColumnVector](arrays.length)
    try {
      arrays.zip(schemas).foreach {
        case (arr, sch) =>
          val fieldVector: FieldVector = Data.importVector(allocator, arr, sch, dictionaryProvider)
          logger.info(
            s"Imported FFI vector: name=${fieldVector.getField.getName}, " +
              s"arrowType=${fieldVector.getField.getType}, " +
              s"vectorClass=${fieldVector.getClass.getSimpleName}, " +
              s"valueCount=${fieldVector.getValueCount}"
          )
          importedVectors += new ArrowColumnVector(fieldVector)
      }
      logger.debug(s"Imported ${importedVectors.size} Arrow vectors with $numRows rows")
      new ColumnarBatch(importedVectors.toArray, numRows)
    } catch {
      case ex: Exception =>
        // Close already-imported vectors to prevent native memory leak
        importedVectors.foreach(v => try v.close() catch { case _: Exception => })
        // Close unconsumed FFI structs
        arrays.drop(importedVectors.size).foreach(a => try a.close() catch { case _: Exception => })
        schemas.drop(importedVectors.size).foreach(s => try s.close() catch { case _: Exception => })
        throw ex
    }
  }

  override def close(): Unit =
    try
      dictionaryProvider.close()
    catch {
      case e: Exception =>
        logger.warn("Error closing Arrow dictionary provider", e)
    }
}

object ArrowFfiBridge {

  /**
   * Global allocator shared across all partition readers (same pattern as DataFusion Comet). RootAllocator is
   * thread-safe. Long.MaxValue = no allocation limit.
   */
  lazy val allocator: RootAllocator = new RootAllocator(Long.MaxValue)

  /** For testing only: close the global allocator to detect leaks. */
  private[spark] def closeForTesting(): Unit = allocator.close()
}
