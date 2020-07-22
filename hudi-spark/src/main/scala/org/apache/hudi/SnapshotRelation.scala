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

import org.apache.hudi.avro.HoodieAvroUtils
import org.apache.hudi.common.model.HoodieBaseFile
import org.apache.hudi.common.table.{HoodieTableMetaClient, TableSchemaResolver}
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMaxCompactionMemoryInBytes

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

case class HoodieMergeOnReadFileSplit(dataFile: PartitionedFile,
                                      logPaths: Option[List[String]],
                                      latestCommit: String,
                                      tablePath: String,
                                      maxCompactionMemoryInBytes: Long,
                                      payload: String,
                                      orderingVal: String)

class SnapshotRelation (val sqlContext: SQLContext,
                        val optParams: Map[String, String],
                        val userSchema: StructType,
                        val globPaths: Seq[Path],
                        val metaClient: HoodieTableMetaClient)
  extends BaseRelation with TableScan with Logging {

  private val conf = sqlContext.sparkContext.hadoopConfiguration

  // use schema from latest metadata, if not present, read schema from the data file
  private val latestSchema = {
    val schemaUtil = new TableSchemaResolver(metaClient)
    val tableSchema = HoodieAvroUtils.createHoodieWriteSchema(schemaUtil.getTableAvroSchemaWithoutMetadataFields)
    AvroConversionUtils.convertAvroSchemaToStructType(tableSchema)
  }
  private val payload = optParams.getOrElse(
    DataSourceReadOptions.MERGE_ON_READ_PAYLOAD_KEY,
    DataSourceReadOptions.DEFAULT_MERGE_ON_READ_PAYLOAD_VAL)
  private val orderingVal = optParams.getOrElse(
    DataSourceReadOptions.MERGE_ON_READ_ORDERING_KEY,
    DataSourceReadOptions.DEFAULT_MERGE_ON_READ_ORDERING_VAL)
  private val maxCompactionMemoryInBytes = getMaxCompactionMemoryInBytes(new JobConf(conf))
  private val fileIndex = buildFileIndex()

  override def schema: StructType = latestSchema

  override def needConversion: Boolean = false

  override def buildScan(): RDD[Row] = {
    val parquetReaderFunction = new ParquetFileFormat().buildReaderWithPartitionValues(
      sparkSession = sqlContext.sparkSession,
      dataSchema = latestSchema,
      partitionSchema = StructType(Nil),
      requiredSchema = latestSchema,
      filters = Seq.empty,
      options = Map.empty,
      hadoopConf = sqlContext.sparkSession.sessionState.newHadoopConf()
    )
    val rdd = new HoodieMergeOnReadRDD(sqlContext.sparkContext,
      sqlContext.sparkSession.sessionState.newHadoopConf(),
      parquetReaderFunction, latestSchema, fileIndex)
    rdd.asInstanceOf[RDD[Row]]
  }

  def buildFileIndex(): List[HoodieMergeOnReadFileSplit] = {
    val inMemoryFileIndex = HudiSparkUtils.createInMemoryFileIndex(sqlContext.sparkSession, globPaths)
    val fileStatuses = inMemoryFileIndex.allFiles()
    if (fileStatuses.isEmpty) {
      throw new HoodieException("No files found for reading in user provided path.")
    }

    val fsView = new HoodieTableFileSystemView(metaClient,
      metaClient.getActiveTimeline.getCommitsTimeline
        .filterCompletedInstants, fileStatuses.toArray)
    val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList
    val latestCommit = fsView.getLastInstant.get().getTimestamp
    val fileGroup = HoodieRealtimeInputFormatUtils.groupLogsByBaseFile(conf, latestFiles.asJava).asScala
    val fileSplits = fileGroup.map(kv => {
      val baseFile = kv._1
      val logPaths = if (kv._2.isEmpty) Option.empty else Option(kv._2.asScala.toList)
      val partitionedFile = PartitionedFile(InternalRow.empty, baseFile.getPath, 0, baseFile.getFileLen)
      HoodieMergeOnReadFileSplit(partitionedFile, logPaths, latestCommit,
        metaClient.getBasePath, maxCompactionMemoryInBytes, payload, orderingVal)
    }).toList
    fileSplits
  }
}
