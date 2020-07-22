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

package org.apache.hudi

import org.apache.hudi.common.config.SerializableConfiguration
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.log.{HoodieMergedLogRecordScanner, LogReaderUtils}
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, IndexedRecord}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer, SchemaConverters}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.util.Try

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

class HoodieMergeOnReadRDD(sc: SparkContext,
                           broadcastedConf: Broadcast[SerializableConfiguration],
                           baseFileReadFunction: PartitionedFile => Iterator[Any],
                           dataSchema: StructType,
                           hoodieRealtimeFileSplits: List[HoodieMergeOnReadFileSplit])
  extends RDD[InternalRow](sc, Nil) {

  // Broadcast the hadoop Configuration to executors.
  def this(sc: SparkContext,
           config: Configuration,
           dataReadFunction: PartitionedFile => Iterator[Any],
           dataSchema: StructType,
           hoodieRealtimeFileSplits: List[HoodieMergeOnReadFileSplit]) = {
    this(
      sc,
      sc.broadcast(new SerializableConfiguration(config))
      .asInstanceOf[Broadcast[SerializableConfiguration]],
      dataReadFunction,
      dataSchema,
      hoodieRealtimeFileSplits)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeParquetPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    val baseFileIterator = read(mergeParquetPartition.split.dataFile, baseFileReadFunction)
    mergeParquetPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logPaths.isEmpty =>
        baseFileIterator
      case unMergeSplit if unMergeSplit.payload
        .equals(DataSourceReadOptions.DEFAULT_MERGE_ON_READ_PAYLOAD_VAL) =>
        unMergeFileIterator(unMergeSplit, baseFileIterator)
      case mergeSplit if !mergeSplit.payload.isEmpty =>
        mergeFileIterator(mergeSplit, baseFileIterator)
      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${mergeParquetPartition.split.dataFile.filePath}" +
        s"log paths: ${mergeParquetPartition.split.logPaths.toString}" +
        s"hoodie table path: ${mergeParquetPartition.split.tablePath}" +
        s"spark partition Index: ${mergeParquetPartition.index}")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    hoodieRealtimeFileSplits.zipWithIndex.map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray
  }

  private def getConfig(): Configuration = {
    broadcastedConf.value.get()
  }

  private def read(partitionedFile: PartitionedFile,
                   readFileFunction: PartitionedFile => Iterator[Any]): Iterator[InternalRow] = {
    val fileIterator = readFileFunction(partitionedFile)
    val rows = fileIterator.flatMap(_ match {
      case r: InternalRow => Seq(r)
      case b: ColumnarBatch => b.rowIterator().asScala
    })
    rows
  }

  private def unMergeFileIterator(split: HoodieMergeOnReadFileSplit,
                                  baseFileIterator: Iterator[InternalRow]): Iterator[InternalRow] =
    new Iterator[InternalRow] {
      private val logSchema = getLogAvroSchema(split)
      private val sparkTypes = SchemaConverters.toSqlType(logSchema).dataType.asInstanceOf[StructType]
      private val converter = new AvroDeserializer(logSchema, sparkTypes)
      private val logRecords = scanLog(split, logSchema).getRecords
      private val logRecordsIterator = logRecords.keySet().iterator().asScala

      override def hasNext: Boolean = {
        baseFileIterator.hasNext || logRecordsIterator.hasNext
      }

      override def next(): InternalRow = {
        if (baseFileIterator.hasNext) {
          baseFileIterator.next()
        } else {
          val curAvrokey = logRecordsIterator.next()
          val curAvroRecord = logRecords.get(curAvrokey).getData.getInsertValue(logSchema).get()
          converter.deserialize(curAvroRecord).asInstanceOf[InternalRow]
        }
      }
    }

  private def mergeFileIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileIterator: Iterator[InternalRow]): Iterator[InternalRow] =
    new Iterator[InternalRow] {
      private val avroSchema = getLogAvroSchema(split)
      private val sparkSchema = SchemaConverters.toSqlType(avroSchema).dataType.asInstanceOf[StructType]
      private val avroToRowConverter = new AvroDeserializer(avroSchema, sparkSchema)
      private val rowToAvroConverter = new AvroSerializer(sparkSchema, avroSchema, false)
      private val logRecords = scanLog(split, avroSchema).getRecords
      private val logRecordToRead = logRecords.keySet()

      private var baseFileFinished = false
      private var logRecordsIterator: Iterator[String] = _
      private var recordToLoad: InternalRow = _

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (baseFileIterator.hasNext) {
          val curRow = baseFileIterator.next()
          val curKey = curRow.getString(HOODIE_RECORD_KEY_COL_POS)
          if (logRecords.containsKey(curKey)) {
            logRecordToRead.remove(curKey)
            val mergedRow = mergeRowWithLog(curRow)
            if (mergedRow.equals(InternalRow.empty)) {
              this.hasNext
            } else {
              recordToLoad = mergedRow
              true
            }
          } else {
            recordToLoad = curRow
            true
          }
        } else {
          if (!baseFileFinished) {
            baseFileFinished = true
            logRecordsIterator = logRecordToRead.iterator().asScala
          }
          if (logRecordsIterator.hasNext) {
            val curKey = logRecordsIterator.next()
            recordToLoad = avroToRowConverter.deserialize(getLogRecordByKey(curKey)).asInstanceOf[InternalRow]
            true
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = {
        recordToLoad
      }

      private def getLogRecordByKey(curKey: String): IndexedRecord = {
        logRecords.get(curKey).getData.getInsertValue(avroSchema).get()
      }

      private def indexRecordToRow(avroRecord: IndexedRecord): InternalRow = {
        avroToRowConverter.deserialize(avroRecord).asInstanceOf[InternalRow]
      }

      private def rowToGenericRecord(curRow: InternalRow): GenericRecord = {
        rowToAvroConverter.serialize(curRow).asInstanceOf[GenericRecord]
      }

      private def mergeRowWithLog(curRow: InternalRow): InternalRow = {
        val curKey = curRow.getString(HOODIE_RECORD_KEY_COL_POS)
        val curAvroRecord = getLogRecordByKey(curKey).asInstanceOf[GenericRecord]
        val historyAvroRecord = rowToGenericRecord(curRow)
        val curPayload = DataSourceUtils
          .createPayload(split.payload, curAvroRecord,
            DataSourceUtils.getNestedFieldVal(curAvroRecord, split.orderingVal, false)
              .asInstanceOf[Comparable[_]])
        val combinedAvro = curPayload.combineAndGetUpdateValue(historyAvroRecord, avroSchema)
        if (combinedAvro.isPresent) indexRecordToRow(combinedAvro.get()) else InternalRow.empty
      }
    }

  private def scanLog(split: HoodieMergeOnReadFileSplit, logSchema: Schema): HoodieMergedLogRecordScanner = {
    val config = getConfig()
    val fs = FSUtils.getFs(split.tablePath, config)
    new HoodieMergedLogRecordScanner(
      fs,
      split.tablePath,
      split.logPaths.get.asJava,
      logSchema,
      split.latestCommit,
      split.maxCompactionMemoryInBytes,
      Try(config.get(HoodieRealtimeConfig.COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP,
        HoodieRealtimeConfig.DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED).toBoolean).getOrElse(false),
      false,
      config.getInt(HoodieRealtimeConfig.MAX_DFS_STREAM_BUFFER_SIZE_PROP,
        HoodieRealtimeConfig.DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE),
      config.get(HoodieRealtimeConfig.SPILLABLE_MAP_BASE_PATH_PROP,
        HoodieRealtimeConfig.DEFAULT_SPILLABLE_MAP_BASE_PATH))
  }

  private def getLogAvroSchema(split: HoodieMergeOnReadFileSplit): Schema = {
    val config = getConfig()
    LogReaderUtils.readLatestSchemaFromLogFiles(split.tablePath, split.logPaths.get.asJava, config)
  }
}
