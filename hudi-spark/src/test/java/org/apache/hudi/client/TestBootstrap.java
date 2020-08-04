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

package org.apache.hudi.client;

import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.FullRecordBootstrapDataProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.FullRecordBootstrapModeSelector;
import org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.testutils.HoodieTestDataGenerator;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.RawTripTestPayload;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetReaderIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieBootstrapConfig;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hudi.index.HoodieIndex.IndexType;
import org.apache.hudi.keygen.NonpartitionedKeyGenerator;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.table.action.bootstrap.BootstrapUtils;
import org.apache.hudi.testutils.HoodieClientTestBase;
import org.apache.hudi.testutils.HoodieMergeOnReadTestUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Random;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toList;
import static org.apache.hudi.common.testutils.HoodieTestDataGenerator.generateGenericRecord;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.apache.spark.sql.functions.callUDF;

/**
 * Tests Bootstrap Client functionality.
 */
public class TestBootstrap extends HoodieClientTestBase {

  public static final String TRIP_HIVE_COLUMN_TYPES = "double,string,string,string,double,double,double,double,"
      + "struct<amount:double,currency:string>,array<struct<amount:double,currency:string>>,boolean";

  @TempDir
  public java.nio.file.Path tmpFolder;

  protected String bootstrapBasePath = null;

  private HoodieParquetInputFormat roInputFormat;
  private JobConf roJobConf;

  private HoodieParquetRealtimeInputFormat rtInputFormat;
  private JobConf rtJobConf;
  private SparkSession spark;

  @BeforeEach
  public void setUp() throws Exception {
    bootstrapBasePath = tmpFolder.toAbsolutePath().toString() + "/data";
    initPath();
    spark = SparkSession.builder()
        .appName("Bootstrap test")
        .master("local[2]")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate();
    jsc = new JavaSparkContext(spark.sparkContext());
    sqlContext = spark.sqlContext();
    hadoopConf = spark.sparkContext().hadoopConfiguration();
    initTestDataGenerator();
    initMetaClient();
    // initialize parquet input format
    reloadInputFormats();
  }

  @AfterEach
  public void tearDown() throws IOException {
    cleanupClients();
    cleanupTestDataGenerator();
  }

  private void reloadInputFormats() {
    // initialize parquet input format
    roInputFormat = new HoodieParquetInputFormat();
    roJobConf = new JobConf(jsc.hadoopConfiguration());
    roInputFormat.setConf(roJobConf);

    rtInputFormat = new HoodieParquetRealtimeInputFormat();
    rtJobConf = new JobConf(jsc.hadoopConfiguration());
    rtInputFormat.setConf(rtJobConf);
  }

  public Schema generateNewDataSetAndReturnSchema(double timestamp, int numRecords, List<String> partitionPaths,
      String srcPath) throws Exception {
    boolean isPartitioned = partitionPaths != null && !partitionPaths.isEmpty();
    Dataset<Row> df = generateTestRawTripDataset(timestamp, numRecords, partitionPaths, jsc, sqlContext);
    df.printSchema();
    if (isPartitioned) {
      df.write().partitionBy("datestr").format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    } else {
      df.write().format("parquet").mode(SaveMode.Overwrite).save(srcPath);
    }
    String filePath = FileStatusUtils.toPath(BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), srcPath,
        (status) -> status.getName().endsWith(".parquet")).stream().findAny().map(p -> p.getValue().stream().findAny())
        .orElse(null).get().getPath()).toString();
    ParquetFileReader reader = ParquetFileReader.open(metaClient.getHadoopConf(), new Path(filePath));
    MessageType schema = reader.getFooter().getFileMetaData().getSchema();
    return new AvroSchemaConverter().convert(schema);
  }

  @Test
  public void testMetadataBootstrapUnpartitionedCOW() throws Exception {
    LOG.error("testMetadataBootstrapUnpartitionedCOW started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(false, false, EffectiveMode.METADATA_BOOTSTRAP_MODE);
    LOG.error("testMetadataBootstrapUnpartitionedCOW ended");
  }

  @Test
  public void testMetadataBootstrapWithUpdatesCOW() throws Exception {
    LOG.error("testMetadataBootstrapWithUpdatesCOW started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, false, EffectiveMode.METADATA_BOOTSTRAP_MODE);
    LOG.error("testMetadataBootstrapWithUpdatesCOW ended");
  }

  private enum EffectiveMode {
    FULL_BOOTSTRAP_MODE,
    METADATA_BOOTSTRAP_MODE,
    MIXED_BOOTSTRAP_MODE
  }

  private void testBootstrapCommon(boolean partitioned, boolean deltaCommit, EffectiveMode mode) throws Exception {

    if (deltaCommit) {
      metaClient = HoodieTestUtils.init(basePath, HoodieTableType.MERGE_ON_READ, bootstrapBasePath);
    } else {
      metaClient = HoodieTestUtils.init(basePath, HoodieTableType.COPY_ON_WRITE, bootstrapBasePath);
    }

    int totalRecords = 100;
    String keyGeneratorClass = partitioned ? SimpleKeyGenerator.class.getCanonicalName()
        : NonpartitionedKeyGenerator.class.getCanonicalName();
    final String bootstrapModeSelectorClass;
    final String bootstrapCommitInstantTs;
    final boolean checkNumRawFiles;
    final boolean isBootstrapIndexCreated;
    final int numInstantsAfterBootstrap;
    final List<String> bootstrapInstants;
    switch (mode) {
      case FULL_BOOTSTRAP_MODE:
        bootstrapModeSelectorClass = FullRecordBootstrapModeSelector.class.getCanonicalName();
        bootstrapCommitInstantTs = HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = false;
        isBootstrapIndexCreated = false;
        numInstantsAfterBootstrap = 1;
        bootstrapInstants = Arrays.asList(bootstrapCommitInstantTs);
        break;
      case METADATA_BOOTSTRAP_MODE:
        bootstrapModeSelectorClass = MetadataOnlyBootstrapModeSelector.class.getCanonicalName();
        bootstrapCommitInstantTs = HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = true;
        isBootstrapIndexCreated = true;
        numInstantsAfterBootstrap = 1;
        bootstrapInstants = Arrays.asList(bootstrapCommitInstantTs);
        break;
      default:
        bootstrapModeSelectorClass = TestRandomBootstapModeSelector.class.getName();
        bootstrapCommitInstantTs = HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS;
        checkNumRawFiles = false;
        isBootstrapIndexCreated = true;
        numInstantsAfterBootstrap = 2;
        bootstrapInstants = Arrays.asList(HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
            HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS);
        break;
    }
    List<String> partitions = Arrays.asList("2020/04/01", "2020/04/02", "2020/04/03");
    double timestamp = new Double(Instant.now().toEpochMilli()).longValue();
    Schema schema = generateNewDataSetAndReturnSchema(timestamp, totalRecords, partitions, bootstrapBasePath);
    HoodieWriteConfig config = getConfigBuilder(schema.toString())
        .withAutoCommit(true)
        .withSchema(schema.toString())
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withMaxNumDeltaCommitsBeforeCompaction(1)
            .build())
        .withBootstrapConfig(HoodieBootstrapConfig.newBuilder()
            .withBootstrapBasePath(bootstrapBasePath)
            .withBootstrapKeyGenClass(keyGeneratorClass)
            .withFullBootstrapInputProvider(TestFullBootstrapDataProvider.class.getName())
            .withBootstrapParallelism(3)
            .withBootstrapModeSelector(bootstrapModeSelectorClass).build())
        .build();
    HoodieWriteClient client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());
    checkBootstrapResults(totalRecords, schema, bootstrapCommitInstantTs, checkNumRawFiles, numInstantsAfterBootstrap,
        numInstantsAfterBootstrap, timestamp, timestamp, deltaCommit, bootstrapInstants);

    // Rollback Bootstrap
    FSUtils.deleteInstantFile(metaClient.getFs(), metaClient.getMetaPath(), new HoodieInstant(State.COMPLETED,
        deltaCommit ? HoodieTimeline.DELTA_COMMIT_ACTION : HoodieTimeline.COMMIT_ACTION, bootstrapCommitInstantTs));
    client.rollBackInflightBootstrap();
    metaClient.reloadActiveTimeline();
    assertEquals(0, metaClient.getCommitsTimeline().countInstants());
    assertEquals(0L, BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), basePath,
        (status) -> status.getName().endsWith(".parquet")).stream().flatMap(f -> f.getValue().stream()).count());

    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    assertFalse(index.useIndex());

    // Run bootstrap again
    client = new HoodieWriteClient(jsc, config);
    client.bootstrap(Option.empty());

    metaClient.reloadActiveTimeline();
    index = BootstrapIndex.getBootstrapIndex(metaClient);
    if (isBootstrapIndexCreated) {
      assertTrue(index.useIndex());
    } else {
      assertFalse(index.useIndex());
    }

    checkBootstrapResults(totalRecords, schema, bootstrapCommitInstantTs, checkNumRawFiles, numInstantsAfterBootstrap,
        numInstantsAfterBootstrap, timestamp, timestamp, deltaCommit, bootstrapInstants);

    // Upsert case
    double updateTimestamp = new Double(Instant.now().toEpochMilli()).longValue();
    String updateSPath = tmpFolder.toAbsolutePath().toString() + "/data2";
    generateNewDataSetAndReturnSchema(updateTimestamp, totalRecords, partitions, updateSPath);
    JavaRDD<HoodieRecord> updateBatch =
        generateInputBatch(jsc, BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), updateSPath,
            (status) -> status.getName().endsWith("parquet")), schema);
    String newInstantTs = client.startCommit();
    client.upsert(updateBatch, newInstantTs);
    checkBootstrapResults(totalRecords, schema, newInstantTs, false, numInstantsAfterBootstrap + 1,
        updateTimestamp, deltaCommit ? timestamp : updateTimestamp, deltaCommit);

    if (deltaCommit) {
      Option<String> compactionInstant = client.scheduleCompaction(Option.empty());
      assertTrue(compactionInstant.isPresent());
      client.compact(compactionInstant.get());
      checkBootstrapResults(totalRecords, schema, compactionInstant.get(), checkNumRawFiles,
          numInstantsAfterBootstrap + 2, 2, updateTimestamp, updateTimestamp, !deltaCommit,
          Arrays.asList(compactionInstant.get()));
    }
  }

  @Test
  public void testMetadataBootstrapWithUpdatesMOR() throws Exception {
    LOG.error("testMetadataBootstrapWithUpdatesMOR started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, true, EffectiveMode.METADATA_BOOTSTRAP_MODE);
    LOG.error("testMetadataBootstrapWithUpdatesMOR ended");
  }

  @Test
  public void testFullBoostrapOnlyCOW() throws Exception {
    LOG.error("testFullBoostrapOnlyCOW started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, false, EffectiveMode.FULL_BOOTSTRAP_MODE);
    LOG.error("testFullBoostrapOnlyCOW ended");
  }

  @Test
  public void testFullBootstrapWithUpdatesMOR() throws Exception {
    LOG.error("testFullBootstrapWithUpdatesMOR started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, true, EffectiveMode.FULL_BOOTSTRAP_MODE);
    LOG.error("testFullBootstrapWithUpdatesMOR ended");
  }

  @Test
  public void testMetaAndFullBoostrapCOW() throws Exception {
    LOG.error("testMetaAndFullBoostrapCOW started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, false, EffectiveMode.MIXED_BOOTSTRAP_MODE);
    LOG.error("testMetaAndFullBoostrapCOW ended");
  }

  @Test
  public void testMetadataAndFullBootstrapWithUpdatesMOR() throws Exception {
    LOG.error("testMetadataAndFullBootstrapWithUpdatesMOR started. Paths :" + basePath + ", " + bootstrapBasePath);
    testBootstrapCommon(true, true, EffectiveMode.MIXED_BOOTSTRAP_MODE);
    LOG.error("testMetadataAndFullBootstrapWithUpdatesMOR ended");
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String maxInstant, boolean checkNumRawFiles,
      int expNumInstants, double expTimestamp, double expROTimestamp, boolean isDeltaCommit) throws Exception {
    checkBootstrapResults(totalRecords, schema, maxInstant, checkNumRawFiles, expNumInstants, expNumInstants,
        expTimestamp, expROTimestamp, isDeltaCommit, Arrays.asList(maxInstant));
  }

  private void checkBootstrapResults(int totalRecords, Schema schema, String instant, boolean checkNumRawFiles,
      int expNumInstants, int numVersions, double expTimestamp, double expROTimestamp, boolean isDeltaCommit,
      List<String> instantsWithValidRecords) throws Exception {
    metaClient.reloadActiveTimeline();
    assertEquals(expNumInstants, metaClient.getCommitsTimeline().filterCompletedInstants().countInstants());
    assertEquals(instant, metaClient.getActiveTimeline()
        .getCommitsTimeline().filterCompletedInstants().lastInstant().get().getTimestamp());

    sqlContext.clearCache();
    Dataset<Row> bootstrapped = sqlContext.read().format("parquet").load(basePath).cache();
    Dataset<Row> original = sqlContext.read().format("parquet").load(bootstrapBasePath).cache();
    bootstrapped.registerTempTable("bootstrapped");
    original.registerTempTable("original");
    if (checkNumRawFiles) {
      List<HoodieFileStatus> files = BootstrapUtils.getAllLeafFoldersWithFiles(metaClient.getFs(), bootstrapBasePath,
          (status) -> status.getName().endsWith(".parquet"))
          .stream().flatMap(x -> x.getValue().stream()).collect(Collectors.toList());
      LOG.error(("List of file obtained from listing bootstrap base path (" + bootstrapBasePath + ") is : "
          + files.stream().map(f -> f.getPath().getUri()).collect(Collectors.joining(","))));
      List<Row> rows = sqlContext.sql("select distinct _hoodie_file_name from bootstrapped").collectAsList();
      LOG.error(("File List from sql query on base path (" + basePath + ") is : "
          + rows.stream().map(r -> r.getString(0)).collect(Collectors.joining(","))));
      assertEquals(files.size() * numVersions, rows.size());
    }

    if (!isDeltaCommit) {
      String predicate = String.join(", ",
          instantsWithValidRecords.stream().map(p -> "\"" + p + "\"").collect(Collectors.toList()));
      assertEquals(totalRecords, sqlContext.sql("select * from bootstrapped where _hoodie_commit_time IN "
          + "(" + predicate + ")").count());
      Dataset<Row> missingOriginal = sqlContext.sql("select a._row_key from original a where a._row_key not "
          + "in (select _hoodie_record_key from bootstrapped)");
      assertEquals(0, missingOriginal.count());
      Dataset<Row> missingBootstrapped = sqlContext.sql("select a._hoodie_record_key from bootstrapped a "
          + "where a._hoodie_record_key not in (select _row_key from original)");
      assertEquals(0, missingBootstrapped.count());
      //sqlContext.sql("select * from bootstrapped").show(10, false);
    }

    // RO Input Format Read
    reloadInputFormats();
    List<GenericRecord> records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>());
    assertEquals(totalRecords, records.size());
    Set<String> seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertEquals(r.get("_row_key").toString(), r.get("_hoodie_record_key").toString(), "Record :" + r);
      assertEquals(expROTimestamp, ((DoubleWritable)r.get("timestamp")).get(), 0.1, "Record :" + r);
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema,  TRIP_HIVE_COLUMN_TYPES, false, new ArrayList<>());
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertEquals(r.get("_row_key").toString(), r.get("_hoodie_record_key").toString(), "Realtime Record :" + r);
      assertEquals(expTimestamp, ((DoubleWritable)r.get("timestamp")).get(),0.1, "Realtime Record :" + r);
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES,
        true, HoodieRecord.HOODIE_META_COLUMNS);
    assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read - Project only Hoodie Columns
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema,  TRIP_HIVE_COLUMN_TYPES, true,
        HoodieRecord.HOODIE_META_COLUMNS);
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_hoodie_record_key").toString()));
      seenKeys.add(r.get("_hoodie_record_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    // RO Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, roJobConf, false, schema, TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    assertEquals(totalRecords, records.size());
    seenKeys = new HashSet<>();
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());

    //RT Input Format Read - Project only non-hoodie column
    reloadInputFormats();
    seenKeys = new HashSet<>();
    records = HoodieMergeOnReadTestUtils.getRecordsUsingInputFormat(
        jsc.hadoopConfiguration(),
        FSUtils.getAllPartitionPaths(metaClient.getFs(), basePath, false).stream()
            .map(f -> basePath + "/" + f).collect(Collectors.toList()),
        basePath, rtJobConf, true, schema,  TRIP_HIVE_COLUMN_TYPES, true,
        Arrays.asList("_row_key"));
    assertEquals(totalRecords, records.size());
    for (GenericRecord r : records) {
      assertFalse(seenKeys.contains(r.get("_row_key").toString()));
      seenKeys.add(r.get("_row_key").toString());
    }
    assertEquals(totalRecords, seenKeys.size());
  }

  public static class TestFullBootstrapDataProvider extends FullRecordBootstrapDataProvider {

    public TestFullBootstrapDataProvider(TypedProperties props, JavaSparkContext jsc) {
      super(props, jsc);
    }

    @Override
    public JavaRDD<HoodieRecord> generateInputRecordRDD(String tableName, String sourceBasePath,
        List<Pair<String, List<HoodieFileStatus>>> partitionPaths) {
      String filePath = FileStatusUtils.toPath(partitionPaths.stream().flatMap(p -> p.getValue().stream())
          .findAny().get().getPath()).toString();
      ParquetFileReader reader = null;
      try {
        reader = ParquetFileReader.open(jsc.hadoopConfiguration(), new Path(filePath));
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage(), e);
      }
      MessageType parquetSchema = reader.getFooter().getFileMetaData().getSchema();
      Schema schema =  new AvroSchemaConverter().convert(parquetSchema);
      return generateInputBatch(jsc, partitionPaths, schema);
    }
  }

  private static JavaRDD<HoodieRecord> generateInputBatch(JavaSparkContext jsc,
      List<Pair<String, List<HoodieFileStatus>>> partitionPaths, Schema writerSchema) {
    List<Pair<String, Path>> fullFilePathsWithPartition = partitionPaths.stream().flatMap(p -> p.getValue().stream()
        .map(x -> Pair.of(p.getKey(), FileStatusUtils.toPath(x.getPath())))).collect(Collectors.toList());
    return jsc.parallelize(fullFilePathsWithPartition.stream().flatMap(p -> {
      try {
        Configuration conf = jsc.hadoopConfiguration();
        AvroReadSupport.setAvroReadSchema(conf, writerSchema);
        Iterator<GenericRecord> recIterator = new ParquetReaderIterator(
            AvroParquetReader.<GenericRecord>builder(p.getValue()).withConf(conf).build());
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(recIterator, 0), false).map(gr -> {
          try {
            String key = gr.get("_row_key").toString();
            String pPath = p.getKey();
            return new HoodieRecord<>(new HoodieKey(key, pPath), new RawTripTestPayload(gr.toString(), key, pPath,
                HoodieTestDataGenerator.TRIP_EXAMPLE_SCHEMA));
          } catch (IOException e) {
            throw new HoodieIOException(e.getMessage(), e);
          }
        });
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    }).collect(Collectors.toList()));
  }

  public static class TestRandomBootstapModeSelector extends BootstrapModeSelector {

    private int currIdx = new Random().nextInt(2);

    public TestRandomBootstapModeSelector(HoodieWriteConfig writeConfig) {
      super(writeConfig);
    }

    @Override
    public Map<BootstrapMode, List<String>> select(List<Pair<String, List<HoodieFileStatus>>> partitions) {
      List<Pair<BootstrapMode, String>> selections = new ArrayList<>();
      partitions.stream().forEach(p -> {
        final BootstrapMode mode;
        if (currIdx == 0) {
          mode = BootstrapMode.METADATA_ONLY;
        } else {
          mode = BootstrapMode.FULL_RECORD;
        }
        currIdx = (currIdx + 1) % 2;
        selections.add(Pair.of(mode, p.getKey()));
      });
      return selections.stream().collect(Collectors.groupingBy(Pair::getKey, mapping(Pair::getValue, toList())));
    }
  }

  public HoodieWriteConfig.Builder getConfigBuilder(String schemaStr) {
    HoodieWriteConfig.Builder builder = getConfigBuilder(schemaStr, IndexType.BLOOM)
        .withExternalSchemaTrasformation(true);
    TypedProperties properties = new TypedProperties();
    properties.setProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY(), "_row_key");
    properties.setProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY(), "datestr");
    builder = builder.withProps(properties);
    return builder;
  }

  private static Dataset<Row> generateTestRawTripDataset(double timestamp, int numRecords, List<String> partitionPaths,
                                                         JavaSparkContext jsc, SQLContext sqlContext) {
    boolean isPartitioned = partitionPaths != null && !partitionPaths.isEmpty();
    final List<String> records = new ArrayList<>();
    IntStream.range(0, numRecords).forEach(i -> {
      String id = "" + i;
      records.add(generateGenericRecord("trip_" + id, "rider_" + id, "driver_" + id,
          timestamp, false, false).toString());
    });
    if (isPartitioned) {
      sqlContext.udf().register("partgen",
          (UDF1<String, String>) (val) -> URLEncoder.encode(partitionPaths.get(
              Integer.parseInt(val.split("_")[1]) % partitionPaths.size()), StandardCharsets.UTF_8.toString()),
          DataTypes.StringType);
    }
    JavaRDD rdd = jsc.parallelize(records);
    Dataset<Row> df = sqlContext.read().json(rdd);
    if (isPartitioned) {
      df = df.withColumn("datestr", callUDF("partgen", new Column("_row_key")));
      // Order the columns to ensure generated avro schema aligns with Hive schema
      df = df.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon",
          "end_lat", "end_lon", "fare", "tip_history", "_hoodie_is_deleted", "datestr");
    } else {
      // Order the columns to ensure generated avro schema aligns with Hive schema
      df = df.select("timestamp", "_row_key", "rider", "driver", "begin_lat", "begin_lon",
          "end_lat", "end_lon", "fare", "tip_history", "_hoodie_is_deleted");
    }
    return df;
  }
}
