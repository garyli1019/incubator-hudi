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
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.config.HoodieRealtimeConfig
import org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS

import org.apache.avro.Schema
import org.apache.avro.generic.{GenericRecord, GenericRecordBuilder}
import org.apache.hadoop.conf.Configuration
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.avro.{AvroDeserializer, AvroSerializer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.Try

case class HoodieMergeOnReadPartition(index: Int, split: HoodieMergeOnReadFileSplit) extends Partition

class HoodieMergeOnReadRDD(sc: SparkContext,
                           broadcastedConf: Broadcast[SerializableConfiguration],
                           fullSchemaFileReader: PartitionedFile => Iterator[Any],
                           requiredSchemaFileReader: PartitionedFile => Iterator[Any],
                           tableState: HoodieMergeOnReadTableState)
  extends RDD[InternalRow](sc, Nil) {

  // Broadcast the hadoop Configuration to executors.
  def this(sc: SparkContext,
           config: Configuration,
           fullSchemaFileReader: PartitionedFile => Iterator[Any],
           requiredSchemaFileReader: PartitionedFile => Iterator[Any],
           tableState: HoodieMergeOnReadTableState) = {
    this(
      sc,
      sc.broadcast(new SerializableConfiguration(config))
      .asInstanceOf[Broadcast[SerializableConfiguration]],
      fullSchemaFileReader,
      requiredSchemaFileReader,
      tableState)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val mergeParquetPartition = split.asInstanceOf[HoodieMergeOnReadPartition]
    mergeParquetPartition.split match {
      case dataFileOnlySplit if dataFileOnlySplit.logPaths.isEmpty =>
        read(mergeParquetPartition.split.dataFile, requiredSchemaFileReader)
      case unMergeSplit if unMergeSplit.payload
        .equals(DataSourceReadOptions.READ_PAYLOAD_CLASS_OPT_UNMERGE_VAL) =>
        unMergeFileIterator(
          unMergeSplit,
          read(mergeParquetPartition.split.dataFile, requiredSchemaFileReader)
        )
      case mergeSplit if !mergeSplit.payload.isEmpty =>
        mergeFileIterator(
          mergeSplit,
          read(mergeParquetPartition.split.dataFile, fullSchemaFileReader)
        )
      case _ => throw new HoodieException(s"Unable to select an Iterator to read the Hoodie MOR File Split for " +
        s"file path: ${mergeParquetPartition.split.dataFile.filePath}" +
        s"log paths: ${mergeParquetPartition.split.logPaths.toString}" +
        s"hoodie table path: ${mergeParquetPartition.split.tablePath}" +
        s"spark partition Index: ${mergeParquetPartition.index}")
    }
  }

  override protected def getPartitions: Array[Partition] = {
    tableState
      .hoodieRealtimeFileSplits
      .zipWithIndex
      .map(file => HoodieMergeOnReadPartition(file._2, file._1)).toArray
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
      private val tableAvroSchema = new Schema.Parser().parse(tableState.tableAvroSchema)
      private val requiredAvroSchema = new Schema.Parser().parse(tableState.requiredAvroSchema)
      private val requiredFieldPosition =
        tableState.requiredStructSchema
          .map(f => tableAvroSchema.getField(f.name).pos()).toList
      private val recordBuilder = new GenericRecordBuilder(requiredAvroSchema)
      private val deserializer = new AvroDeserializer(requiredAvroSchema, tableState.requiredStructSchema)
      private val unsafeProjection = UnsafeProjection.create(tableState.requiredStructSchema)
      private val logRecords = scanLog(split, tableAvroSchema).getRecords
      private val logRecordsKeyIterator = logRecords.keySet().iterator().asScala

      private var recordToLoad: InternalRow = _

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (baseFileIterator.hasNext) {
          recordToLoad = baseFileIterator.next()
          true
        } else {
          if (logRecordsKeyIterator.hasNext) {
            val curAvrokey = logRecordsKeyIterator.next()
            val curAvroRecord = logRecords.get(curAvrokey).getData.getInsertValue(tableAvroSchema)
            if (!curAvroRecord.isPresent) {
              // delete record found, skipping
              this.hasNext
            } else {
              val requiredAvroRecord = AvroConversionUtils
                .buildAvroRecordBySchema(curAvroRecord.get(), requiredAvroSchema, requiredFieldPosition, recordBuilder)
              recordToLoad = unsafeProjection(deserializer.deserialize(requiredAvroRecord).asInstanceOf[InternalRow])
              true
            }
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = {
        recordToLoad
      }
    }

  private def mergeFileIterator(split: HoodieMergeOnReadFileSplit,
                                baseFileIterator: Iterator[InternalRow]): Iterator[InternalRow] =
    new Iterator[InternalRow] {
      private val tableAvroSchema = new Schema.Parser().parse(tableState.tableAvroSchema)
      private val requiredAvroSchema = new Schema.Parser().parse(tableState.requiredAvroSchema)
      private val requiredFieldPosition =
        tableState.requiredStructSchema
          .map(f => tableAvroSchema.getField(f.name).pos()).toList
      private val serializer = new AvroSerializer(tableState.tableStructSchema, tableAvroSchema, false)
      private val requiredDeserializer = new AvroDeserializer(requiredAvroSchema, tableState.requiredStructSchema)
      private val recordBuilder = new GenericRecordBuilder(requiredAvroSchema)
      private val unsafeProjection = UnsafeProjection.create(tableState.requiredStructSchema)
      private val logRecords = scanLog(split, tableAvroSchema).getRecords
      private val logRecordsKeyIterator = logRecords.keySet().iterator().asScala
      private val keyToSkip = mutable.Set.empty[String]

      private var recordToLoad: InternalRow = _

      @scala.annotation.tailrec
      override def hasNext: Boolean = {
        if (baseFileIterator.hasNext) {
          val curRow = baseFileIterator.next()
          val curKey = curRow.getString(HOODIE_RECORD_KEY_COL_POS)
          if (logRecords.containsKey(curKey)) {
            // duplicate key found, merging
            keyToSkip.add(curKey)
            val mergedAvroRecord = mergeRowWithLog(curRow, curKey)
            if (!mergedAvroRecord.isPresent) {
              // deleted
              this.hasNext
            } else {
              // load merged record as InternalRow with required schema
              val requiredAvroRecord = AvroConversionUtils
                .buildAvroRecordBySchema(
                  mergedAvroRecord.get(),
                  requiredAvroSchema,
                  requiredFieldPosition,
                  recordBuilder
                )
              recordToLoad = unsafeProjection(requiredDeserializer
                .deserialize(requiredAvroRecord).asInstanceOf[InternalRow])
              true
            }
          } else {
            // No merge needed, load current row with required schema
            recordToLoad = unsafeProjection(createRowWithRequiredSchema(curRow))
            true
          }
        } else {
          if (logRecordsKeyIterator.hasNext) {
            val curKey = logRecordsKeyIterator.next()
            if (keyToSkip.contains(curKey)) {
              this.hasNext
            } else {
              val insertAvroRecord = getLogRecordByKey(curKey)
              if (!insertAvroRecord.isPresent) {
                // stand alone delete record, skipping
                this.hasNext
              } else {
                val requiredAvroRecord = AvroConversionUtils
                  .buildAvroRecordBySchema(
                    insertAvroRecord.get(),
                    requiredAvroSchema,
                    requiredFieldPosition,
                    recordBuilder
                  )
                recordToLoad = unsafeProjection(requiredDeserializer
                  .deserialize(requiredAvroRecord).asInstanceOf[InternalRow])
                true
              }
            }
          } else {
            false
          }
        }
      }

      override def next(): InternalRow = recordToLoad

      private def createRowWithRequiredSchema(row: InternalRow): InternalRow = {
        val rowToReturn = new SpecificInternalRow(tableState.requiredStructSchema)
        val posIterator = requiredFieldPosition.iterator
        var curIndex = 0
        tableState.requiredStructSchema.foreach(
          f => {
            val curPos = posIterator.next()
            val curField = row.get(curPos, f.dataType)
            rowToReturn.update(curIndex, curField)
            curIndex = curIndex + 1
          }
        )
        rowToReturn
      }

      private def getLogRecordByKey(curKey: String) = {
        logRecords.get(curKey).getData.getInsertValue(tableAvroSchema)
      }

      private def mergeRowWithLog(curRow: InternalRow, curKey: String) = {
        val historyAvroRecord = serializer.serialize(curRow).asInstanceOf[GenericRecord]
        logRecords.get(curKey).getData.combineAndGetUpdateValue(historyAvroRecord, tableAvroSchema)
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
}
