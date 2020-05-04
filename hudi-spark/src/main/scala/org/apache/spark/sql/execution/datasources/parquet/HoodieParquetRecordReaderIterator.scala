/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.execution.datasources.parquet

import org.apache.hudi.realtime.HoodieRealtimeParquetRecordReader
import org.apache.spark.internal.Logging
import org.apache.spark.sql.avro.{AvroDeserializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.types.StructType

import java.io.Closeable

class HoodieParquetRecordReaderIterator(private[this] var rowReader: HoodieRealtimeParquetRecordReader[UnsafeRow])
  extends Iterator[UnsafeRow]
  with Closeable
  with Logging {
  private[this] var havePair = false
  private[this] var finished = false
  private[this] var shouldReadLog = rowReader.internalReader.deltaRecordMap.size() > 0
  private[this] var deltaRecordKeys = rowReader.internalReader.deltaRecordMap.keySet()
  private[this] var avroSchema = rowReader.internalReader.logAvroSchema

  var sparkTypes: StructType = _
  var converter: AvroDeserializer = _
  var deltaIter = deltaRecordKeys.iterator()

  // SPARK-23457 Register a task completion lister before `initialization`.
  // The rowReader has to be initialized after the Iterator constructed
  // So we need to initialize the Iterator after rowReader initialized
  // TODO: Make this less ugly
  def init(): Unit = {
    shouldReadLog = rowReader.internalReader.deltaRecordMap.size() > 0
    deltaRecordKeys = rowReader.internalReader.deltaRecordMap.keySet()
    avroSchema = rowReader.internalReader.logAvroSchema
    sparkTypes = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
    converter = new AvroDeserializer(avroSchema, sparkTypes)
    deltaIter = deltaRecordKeys.iterator()
  }

  override def hasNext: Boolean = {
    // read log record before processing parquet
    if (shouldReadLog) {
      if (deltaIter.hasNext) {
        !finished
      }
      else {
        shouldReadLog = false
        !finished
      }
    } else {
      //org.apache.spark.sql.execution.datasources.FileScanRDD.getNext() check this finished or not
      //next() in this class use havePair to trigger reading next row
      if (!finished && !havePair) {
        // check if next row exist and read next row in rowReader
        finished = !rowReader.nextKeyValue
        // skip if record is in delta map
        while (!finished && skipCurrentValue(rowReader.getCurrentValue)) {
          finished = !rowReader.nextKeyValue
        }
        if (finished) {
          // Close and release the reader here; close() will also be called when the task
          // completes, but for tasks that read from many files, it helps to release the
          // resources early.
          logInfo("closing reader")
          close()
        }
        havePair = !finished
      }
      !finished
    }
  }

  override def next(): UnsafeRow = {
    // read log file before reading parquet
    if (shouldReadLog) {
      getLogRecord()
    } else {
      if (!hasNext) {
        throw new java.util.NoSuchElementException("End of stream")
      }
      havePair = false
      logInfo(s"reading value: ${rowReader.getCurrentValue.getString(2)}")
      rowReader.getCurrentValue
    }
  }

  override def close(): Unit = {
    if (rowReader != null) {
      try {
        rowReader.close()
      } finally {
        rowReader = null
      }
    }
  }

  private def skipCurrentValue(currentValue: UnsafeRow): Boolean = {
    val curKey = currentValue.getString(2)
    if (rowReader.internalReader.deltaRecordMap.keySet().contains(curKey)) {
      logInfo(s"$curKey is in the delta map, skipping")
      true
    } else {
      logInfo(s"$curKey is NOT in the delta map, reading")
      false
    }
  }

  // TODO: Directly deserialize to UnsafeRow
  private def toUnsafeRow(row: InternalRow, schema: StructType): UnsafeRow = {
    val converter = UnsafeProjection.create(schema)
    converter.apply(row)
  }

  private def getLogRecord(): UnsafeRow = {
    val curRecord = rowReader.internalReader.deltaRecordMap.get(deltaIter.next()).getData.getInsertValue(avroSchema).get()
    // Convert Avro GenericRecord to InternalRow
    val curRow = converter.deserialize(curRecord).asInstanceOf[InternalRow]
    // Convert InternalRow to UnsafeRow
    toUnsafeRow(curRow, sparkTypes)
  }
}
