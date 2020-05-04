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

import java.util

import org.apache.hudi.hadoop.HoodieParquetInputFormat
import org.apache.hudi.hadoop.realtime.HoodieRealtimeInputFormatUtils
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.JobConf
import org.apache.hudi.common.model.HoodieCommitMetadata
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.util.ParquetUtils
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.exception.HoodieException
import org.apache.hudi.table.HoodieTable
import org.apache.log4j.LogManager
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * This is the relation to read Hudi MOR table.
 * @param sqlContext
 * @param basePath
 * @param optParams
 * @param userSchema
 */
class RealtimeRelation(val sqlContext: SQLContext,
                       val basePath: String,
                       val optParams: Map[String, String],
                       val userSchema: StructType) extends BaseRelation with TableScan{
  private val log = LogManager.getLogger(classOf[RealtimeRelation])
  private val conf = sqlContext.sparkContext.hadoopConfiguration
  // set config for listStatus() in HoodieParquetInputFormat
  conf.setStrings("mapreduce.input.fileinputformat.inputdir", basePath)
  conf.setStrings("mapreduce.input.fileinputformat.input.dir.recursive", "true")
  private val HoodieInputFormat = new HoodieParquetInputFormat
  HoodieInputFormat.setConf(conf)
  private val fileStatus = HoodieInputFormat.listStatus(new JobConf(conf))
  log.info("all parquet files" + fileStatus.map(s => s.getPath.toString).mkString(","))
  private val realTimeUtils = new HoodieRealtimeInputFormatUtils
  private val fileGroup = realTimeUtils.getRealtimeFileGroup(conf, util.Arrays.stream(fileStatus)).asScala

  // split the file group to: parquet file without a matching log file, parquet file need to merge with log files
  private val parquetWithoutLogPaths: List[String] = fileGroup.filter(p => p._2.size() == 0).keys.toList
  private val fileWithLogMap: Map[String, String] = fileGroup.filter(p => p._2.size() > 0).map{ case(k, v) => (k, v.asScala.toList.mkString(","))}.toMap
  log.info("parquetWithoutLogPaths" + parquetWithoutLogPaths.mkString(","))
  log.info("fileWithLogMap" + fileWithLogMap.map(m => s"${m._1}:${m._2}").mkString(","))

  // add log file map to options
  private val finalOps = optParams ++ fileWithLogMap

  // load Hudi metadata
  val metaClient = new HoodieTableMetaClient(conf, basePath, true)
  private val hoodieTable = HoodieTable.create(metaClient, HoodieWriteConfig.newBuilder().withPath(basePath).build(),
    sqlContext.sparkContext)
  // delta commit
  private val commitTimeline = hoodieTable.getMetaClient.getCommitsAndCompactionTimeline
  if (commitTimeline.empty()) {
    throw new HoodieException("No Valid Hudi timeline exists")
  }
  private val completedCommitTimeline = hoodieTable.getMetaClient.getCommitsTimeline.filterCompletedInstants()
  //log.warn(s"commitTimeline: ${commitTimeline.toString}")
  //log.warn(s"completedCommitTimeline: ${completedCommitTimeline.toString}")
  private val lastInstant = completedCommitTimeline.lastInstant().get()
  // TODO: clean up this. set last commit time
  conf.setStrings("hoodie.realtime.last.commit", lastInstant.getTimestamp)
  // use schema from a file produced in the latest instant
  private val latestSchema = {
    // if latest commit is not a compaction
    val metaFilePath = if (fileWithLogMap.isEmpty) {
      val latestMeta = HoodieCommitMetadata.fromBytes(commitTimeline.getInstantDetails(lastInstant).get, classOf[HoodieCommitMetadata])
      latestMeta.getFileIdAndFullPaths(basePath).values().iterator().next()
    }
    else {
      fileWithLogMap.keys.head
    }
      AvroConversionUtils.convertAvroSchemaToStructType(ParquetUtils.readAvroSchema(
      conf, new Path(metaFilePath)))
    }

  override def schema: StructType = latestSchema

  override def buildScan(): RDD[Row] = {
    // read parquet file doesn't have matching log file to merge as normal parquet
    val regularParquet = sqlContext
        .read
        .options(finalOps)
        .schema(schema)
        .format("parquet")
        .load(parquetWithoutLogPaths:_*)
        .toDF()
    // hudi parquet files needed to merge with log file
    sqlContext
      .read
      .options(finalOps)
      .schema(schema)
      .format("org.apache.spark.sql.execution.datasources.parquet.HoodieRealtimeInputFormat")
      .load(fileWithLogMap.keys.toList:_*)
      .toDF()
      .union(regularParquet)
      .rdd
  }
}
