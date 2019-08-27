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
package org.apache.hadoop.hbase.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.format.TableInfo;
import org.apache.carbondata.sdk.file.CarbonSchemaWriter;
import org.apache.carbondata.sdk.file.CarbonWriter;
import org.apache.carbondata.sdk.file.CarbonWriterBuilder;
import org.apache.carbondata.sdk.file.Schema;

import com.google.common.annotations.VisibleForTesting;
import com.huawei.cloudtable.leo.HBaseTableReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.TableDescriptors;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.replication.BaseReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Threads;
import org.apache.hadoop.hbase.wal.WAL.Entry;
import org.apache.hbase.thirdparty.com.google.common.collect.Maps;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@InterfaceAudience.Private public class CarbonReplicationEndpoint extends BaseReplicationEndpoint {
  private static final Logger LOG = LoggerFactory.getLogger(CarbonReplicationEndpoint.class);

  private static final String CARBON_APPEND_BATCH = "hbase.carbon.append.batch";
  private static final String CARBON_WRITE_BATCH_SIZE_BYTES = "hbase.carbon.write.batch.size.bytes";
  // by default 32MB
  private static final int CARBON_WRITE_BATCH_SIZE_BYTES_DEFAULT = 32 * 1024 * 1024;
  private static final String CARBON_WRITER_KEEP_ALIVE_TIME_MS =
      "hbase.carbon.writer.keepalive.time.ms";
  // by default 5min.
  private static final long CARBON_WRITER_KEEP_ALIVE_TIME_MS_DEFAULT = 5 * 60 * 1000;

  // todo need to add this in hbase-site.xml
  private static final String CARBON_CONFIG_FOLDER = "hbase.carbon.config.folder";

  private static String DELETE = String.valueOf(1);
  private static String DELETEFAMILY = String.valueOf(2);
  private static String INSERT = String.valueOf(0);

  private Configuration conf;
  // Size limit for replication RPCs, in bytes
  private int replicationRpcLimit;

  private int writeBatchSizeBytes;
  // if no data comes in 5min, we close the writer to make sure that all data flush to obs disk.
  private volatile long lastReplicateTime;

  private volatile boolean isWriting;

  private ScheduledExecutorService writeTimeChecker = Executors.newSingleThreadScheduledExecutor();

  // Thread pool executor to write the table into carbon file format
  private ThreadPoolExecutor exec;

  private int maxThreads;

  // HBase table descriptors
  private TableDescriptors tableDescriptors;

  // Map of table and pair of table schema and properties
  private Map<TableName, CarbonHbaseMeta> tableSchemaMap = Maps.newConcurrentMap();

  // Map of regions and carbon writer
  private Map<String, CarbonWriter> regionsWriterMap = Maps.newConcurrentMap();

  private Map<String, CarbonTable> pathCarbonTableMap = Maps.newConcurrentMap();

  private boolean appendBatch;

  private CarbonWriterBuilder builder = null;

  private String taskNo = null;

  private long factTimeStamp = 0;

  @Override public UUID getPeerUUID() {
    return ctx.getClusterId();
  }

  @Override public boolean canReplicateToSameCluster() {
    return true;
  }

  @Override public void start() {
    startAsync();
  }

  @Override public void stop() {
    closeCarbonWriters();
    stopAsync();
  }

  @Override protected void doStart() {
    notifyStarted();
  }

  @Override protected void doStop() {
    notifyStopped();
  }

  @Override public void init(Context context) throws IOException {
    super.init(context);
    conf = ctx.getConfiguration();
    tableDescriptors = ctx.getTableDescriptors();
    this.replicationRpcLimit =
        (int) (0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE, RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    // 32 MB per batch
    int batchSizeBytes =
        conf.getInt(CARBON_WRITE_BATCH_SIZE_BYTES, CARBON_WRITE_BATCH_SIZE_BYTES_DEFAULT);
    this.writeBatchSizeBytes = Math.min(batchSizeBytes, replicationRpcLimit);
    this.lastReplicateTime = System.currentTimeMillis();
    // Initialize the executor
    this.maxThreads = conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
        HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    this.appendBatch = conf.getBoolean(CARBON_APPEND_BATCH, true);
    this.exec = new ThreadPoolExecutor(maxThreads, maxThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.exec.allowCoreThreadTimeOut(true);
    long writerTimeOut =
        conf.getLong(CARBON_WRITER_KEEP_ALIVE_TIME_MS, CARBON_WRITER_KEEP_ALIVE_TIME_MS_DEFAULT);
    writeTimeChecker.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        if (System.currentTimeMillis() - lastReplicateTime > writerTimeOut && !isWriting) {
          LOG.info("No data comes, will close carbon writers.");
          closeCarbonWriters();
        }
      }
    }, 0, 1, TimeUnit.MINUTES);

    try {
      loadCarbonConfigFromFile(conf);
    } catch (Exception e) {
      LOG.warn("Error while loading carbon config, just ignore it. ", e);
    }
  }

  @VisibleForTesting public void init(Configuration conf) throws IOException {
    this.conf = conf;
    this.replicationRpcLimit =
        (int) (0.95 * conf.getLong(RpcServer.MAX_REQUEST_SIZE, RpcServer.DEFAULT_MAX_REQUEST_SIZE));
    // 32 MB per batch
    int batchSizeBytes =
        conf.getInt(CARBON_WRITE_BATCH_SIZE_BYTES, CARBON_WRITE_BATCH_SIZE_BYTES_DEFAULT);
    this.writeBatchSizeBytes = Math.min(batchSizeBytes, replicationRpcLimit);
    this.lastReplicateTime = System.currentTimeMillis();
    // Initialize the executor
    this.maxThreads = conf.getInt(HConstants.REPLICATION_SOURCE_MAXTHREADS_KEY,
        HConstants.REPLICATION_SOURCE_MAXTHREADS_DEFAULT);
    this.appendBatch = conf.getBoolean(CARBON_APPEND_BATCH, true);
    this.exec = new ThreadPoolExecutor(maxThreads, maxThreads, 60, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>());
    this.exec.allowCoreThreadTimeOut(true);
    long writerTimeOut =
        conf.getLong(CARBON_WRITER_KEEP_ALIVE_TIME_MS, CARBON_WRITER_KEEP_ALIVE_TIME_MS_DEFAULT);
    writeTimeChecker.scheduleAtFixedRate(new Runnable() {
      @Override public void run() {
        if (System.currentTimeMillis() - lastReplicateTime > writerTimeOut && !isWriting) {
          LOG.info("No data comes, will close carbon writers.");
          closeCarbonWriters();
        }
      }
    }, 0, 1, TimeUnit.MINUTES);

    try {
      loadCarbonConfigFromFile(conf);
    } catch (Exception e) {
      LOG.warn("Error while loading carbon config, just ignore it. ", e);
    }
  }

  @Override public boolean replicate(ReplicateContext replicateContext) {
    LOG.info("I will replicate to carbon...");
    isWriting = true;
    // update the last writer time.
    CompletionService<Integer> pool = new ExecutorCompletionService<>(this.exec);
    try {
      // parse the replication entries to region wise
      List<List<Entry>> batches = createBatches(replicateContext.getEntries());
      while (this.isRunning() && !exec.isShutdown()) {

        if (!isPeerEnabled()) {
          Threads.sleep(1000);
          continue;
        } else {
          break;
        }
      }
      // TODO: Move this logic, can be done while writing to the file
      // Initialize and cache the carbon writer for each region
      initCarbonWriters(batches);
      // Replicate the entries concurrently based on batches
      parallelReplicate(pool, replicateContext, batches);
    } catch (Exception e) {
      LOG.error("Exception occured while writing the data", e);
      return false;
    }
    isWriting = false;
    return true;
  }

  /**
   * Divide the entries into multiple batches, so that we can replicate each batch in a thread pool
   * concurrently. Note that, for serial replication, we need to make sure that entries from the
   * same region to be replicated serially, so entries from the same region consist of a batch, and
   * we will divide a batch into several batches by replicationRpcLimit in method
   * serialReplicateRegionEntries()
   */
  private List<List<Entry>> createBatches(final List<Entry> entries) {
    Map<byte[], List<Entry>> regionEntries = new TreeMap<>(Bytes.BYTES_COMPARATOR);
    for (Entry entry : entries) {
      // Skip those table entries where CARBON_SCHEMA doesn't exist in table descriptor
      TableName tableName = entry.getKey().getTableName();
      try {
        if (getCarbonSchemaStr(tableName) == null) {
          continue;
        }
      } catch (Exception io) {
        LOG.error("Exception occured while retrieving carbon schema details of table=" + tableName
            .getNameAsString(), io);
        continue;
      }

      regionEntries.computeIfAbsent(entry.getKey().getEncodedRegionName(), key -> new ArrayList<>())
          .add(entry);
    }
    return new ArrayList<>(regionEntries.values());
  }

  public String getCarbonSchemaStr(TableName tableName) throws IOException {
    if (tableDescriptors.get(tableName) == null) {
      LOG.warn("table desc is null: " + tableName.getNameAsString());
      return null;
    }
    return tableDescriptors.get(tableName).getValue(CarbonMasterObserver.CARBON_SCHEMA_DESC);
  }

  private long parallelReplicate(CompletionService<Integer> pool, ReplicateContext replicateContext,
      List<List<Entry>> batches) throws IOException {
    int futures = 0;
    for (int i = 0; i < batches.size(); i++) {
      List<Entry> entries = batches.get(i);
      if (!entries.isEmpty()) {
        LOG.info("Submitting {} entries of total size {}, total batches {}", entries.size(),
            replicateContext.getSize(), batches.size());
        // RuntimeExceptions encountered here bubble up and are handled in ReplicationSource
        pool.submit(createReplicator(entries, i));
        futures++;
      }
    }

    IOException iox = null;
    long lastWriteTime = 0;
    for (int i = 0; i < futures; i++) {
      try {
        // wait for all futures, remove successful parts
        // (only the remaining parts will be retried)
        Future<Integer> f = pool.take();
        int index = f.get();
        List<Entry> batch = batches.get(index);
        batches.set(index, Collections.emptyList()); // remove successful batch
        // Find the most recent write time in the batch
        long writeTime = batch.get(batch.size() - 1).getKey().getWriteTime();
        if (writeTime > lastWriteTime) {
          lastWriteTime = writeTime;
        }
      } catch (InterruptedException ie) {
        iox = new IOException(ie);
      } catch (ExecutionException ee) {
        // cause must be an IOException
        iox = (IOException) ee.getCause();
      }
    }
    if (iox != null) {
      // if we had any exceptions, try again
      throw iox;
    }
    // 5 min close all writer
    if (!appendBatch) {
      LOG.info("Will close carbon writers.");
      closeCarbonWriters();
    }
    this.lastReplicateTime = System.currentTimeMillis();
    return lastWriteTime;
  }

  protected Callable<Integer> createReplicator(List<Entry> entries, int batchIndex) {
    return () -> serialReplicateRegionEntries(entries, batchIndex);
  }

  private int serialReplicateRegionEntries(List<Entry> entries, int batchIndex) throws IOException {
    int batchSize = 0, index = 0;
    List<Entry> batch = new ArrayList<>();
    for (Entry entry : entries) {
      int entrySize = getEstimatedEntrySize(entry);
      if (batchSize > 0 && batchSize + entrySize > writeBatchSizeBytes) {
        writeToCarbonFile(batch, index++);
        batch.clear();
        batchSize = 0;
      }
      batch.add(entry);
      batchSize += entrySize;
    }
    if (batchSize > 0) {
      writeToCarbonFile(batch, index);
    }
    return batchIndex;
  }

  /*
   * Returns approximate entry size
   */
  private int getEstimatedEntrySize(Entry e) {
    long size = e.getKey().estimatedSerializedSizeOf() + e.getEdit().estimatedSerializedSizeOf();
    return (int) size;
  }

  /*
   * Whether peer is enabled
   */
  protected boolean isPeerEnabled() {
    return ctx.getReplicationPeer().isPeerEnabled();
  }

  private void initCarbonWriters(List<List<Entry>> batches) {
    for (List<Entry> entry : batches) {
      if (entry.isEmpty()) {
        continue;
      }

      for (Entry walEntry : entry) {
        // Create carbon writer for each region with the specific path
        String regionName = Bytes.toString(walEntry.getKey().getEncodedRegionName());
        if (regionsWriterMap.get(regionName) == null) {
          try {
            TableName tableName = walEntry.getKey().getTableName();
            createCarbonWriter(tableName, regionName);
          } catch (Exception e) {
            LOG.error("Exception occured while initializing carbon writer for region " + regionName,
                e);
          }
        }
      }
    }
  }

  private void createCarbonWriter(TableName tableName, String regionName)
      throws IOException, InvalidLoadOptionException {

    Schema tableSchema = null;
    Map<String, String> tblproperties = null;
    CarbonHbaseMeta hbaseMeta = tableSchemaMap.get(tableName);
    // Create carbon writer and cache it
    if (hbaseMeta == null) {
      // Get the schema from table desc and convert it into JSON format
      String schemaDesc = getCarbonSchemaStr(tableName);
      LOG.info("Region name is: " + regionName + " Current thread is: " + Thread.currentThread()
          .getName());
      LOG.info("Carbon schema is: " + schemaDesc);
      tblproperties = new HashMap<>();
      tableSchema = CarbonSchemaWriter.convertToSchemaFromJSON(schemaDesc, tblproperties);
      hbaseMeta = new CarbonHbaseMeta(tableSchema, tblproperties);
      tableSchemaMap.put(tableName, hbaseMeta);
    } else {
      tableSchema = hbaseMeta.getSchema();
      tblproperties = hbaseMeta.getTblProperties();
    }
    tblproperties.put(CarbonCommonConstants.PRIMARY_KEY_COLUMNS, hbaseMeta.getPrimaryKeyColumns());
    String tablePath = tblproperties.get(CarbonMasterObserver.PATH);
    String ak = tblproperties.get(SparkS3Constants.FS_OBS_AK);
    String sk = tblproperties.get(SparkS3Constants.FS_OBS_SK);
    String endpoint = tblproperties.get(SparkS3Constants.FS_OBS_END_POINT);

    Map<String, String> clonedProps = new HashMap<>(tblproperties);
    clonedProps.remove(CarbonMasterObserver.HBASE_MAPPING_DETAILS);
    clonedProps.remove(CarbonMasterObserver.PATH);
    clonedProps.remove(SparkS3Constants.FS_OBS_AK, ak);
    clonedProps.remove(SparkS3Constants.FS_OBS_SK, sk);
    clonedProps.remove(SparkS3Constants.FS_OBS_END_POINT, endpoint);
    // set ak sk and endpoint for carbon writer to access obs when filesystem schema is 'obs://'.
    Configuration config = new Configuration(conf);
    config.set(SparkS3Constants.FS_S3_AK, ak);
    config.set(SparkS3Constants.FS_S3_SK, sk);
    config.set(SparkS3Constants.FS_S3_END_POINT, endpoint);
    config.set(SparkS3Constants.FS_OBS_AK, ak);
    config.set(SparkS3Constants.FS_OBS_SK, sk);
    config.set(SparkS3Constants.FS_OBS_END_POINT, endpoint);
    config.set("spark.hadoop.fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    config.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    // set stream segment size to 1GB.
    config.set("carbon.streamsegment.maxsize", "1073741824");

    ThreadLocalSessionInfo.setConfigurationToCurrentThread(config);

    // isPKTable will try to load the schema from thrift file also.
    CarbonTable carbonTable = pathCarbonTableMap.get(tablePath);
    String databaseName = tableName.getNamespaceAsString();
    String carbonTableName = tableName.getQualifierAsString();
    if (carbonTable == null) {
      try {
        String tableMetadataFile = CarbonTablePath.getSchemaFilePath(tablePath);
        FileFactory.FileType fileType = FileFactory.getFileType(tableMetadataFile);
        if (FileFactory.isFileExist(tableMetadataFile, fileType)) {
          TableInfo tableInfo = CarbonUtil.readSchemaFile(tableMetadataFile);
          ThriftWrapperSchemaConverterImpl schemaConverter = new ThriftWrapperSchemaConverterImpl();
          org.apache.carbondata.core.metadata.schema.table.TableInfo wrapperTableInfo = null;
          if (databaseName != null && carbonTableName != null) {
            wrapperTableInfo = schemaConverter
                .fromExternalToWrapperTableInfo(tableInfo, databaseName, carbonTableName,
                    tablePath);
          } else {
            wrapperTableInfo = schemaConverter.fromExternalToWrapperTableInfo(tableInfo, "",
                "_tempTable_" + String.valueOf(System.nanoTime()), tablePath);
          }
          carbonTable = CarbonTable.buildFromTableInfo(wrapperTableInfo);
          pathCarbonTableMap.putIfAbsent(tablePath, carbonTable);
        }
      } catch (Exception e) {
        LOG.error("Error occured while reading thrift schema file for pk table, ", e);
      }
    }

    // todo check and remove these logic when carbontable is there
    String timestampFormat = CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT;
    if (carbonTable.getTableInfo() != null
        && carbonTable.getTableInfo().getFactTable().getTableProperties().get("timestampformat")
        != null) {
      timestampFormat =
          carbonTable.getTableInfo().getFactTable().getTableProperties().get("timestampformat");
    }

    builder = CarbonWriter.builder()
        .outputPath(tablePath)
        .withTable(carbonTable)
        .withTableProperties(clonedProps)
        .withLoadOption("bad_records_action", "force")
        .withLoadOption("timestampformat", timestampFormat)
        .withRowFormat(tableSchema)
        .writtenBy(CarbonReplicationEndpoint.class.getSimpleName())
        .withHadoopConf(config);
    regionsWriterMap.put(regionName, builder.build());

    LOG.info("Path is: " + tablePath + " Region name is: " + regionName + " Thread: " + Thread
        .currentThread().getName());
  }

  private void closeCarbonWriters() {
    try {
      for (CarbonWriter writer : regionsWriterMap.values()) {
        writer.close();
      }
      regionsWriterMap.clear();
    } catch (Exception e) {
      LOG.error("Exception occured while closing the carbon writer", e);
    }
  }

  // Table schema details
  //
  // {
  // �ID�:"INT",
  // �name�:"string",
  // �department�:"string",
  // �salary�:"double"
  // �tblproperties�: {�sort_columns�:"ID",
  // �hbase_mapping�:"key=ID,cf.name=name,cf.dept=department,cf2.sal=salary",
  // �path�:"dlc://user.bucket1/customer"}
  // }
  private void writeToCarbonFile(List<Entry> batch, int index) {
    String regionName = Bytes.toString(batch.get(0).getKey().getEncodedRegionName());
    try {
      // Check and create the write if not exist
      TableName tName = batch.get(index).getKey().getTableName();
      CarbonWriter carbonWriter = regionsWriterMap.get(regionName);
      if (carbonWriter == null) {
        createCarbonWriter(tName, regionName);
        carbonWriter = regionsWriterMap.get(regionName);
      }
      CarbonHbaseMeta hbaseMeta = tableSchemaMap.get(tName);
      DataTypeConverter converter = hbaseMeta.getDataTypeConverter();
      // Check the keys in sequence order and if the keys are same then merge to the same carbon row
      ByteArrayWrapper rowKey = null;
      String[] row = null;
      for (Entry entry : batch) {
        for (Cell cell : entry.getEdit().getCells()) {
          ByteArrayWrapper tmpKey =
              new ByteArrayWrapper(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(),
                  cell.getType());
          boolean deleteFamily = cell.getType() == Cell.Type.DeleteFamily;
          boolean delete = cell.getType() == Cell.Type.Delete;
          boolean merge = false;
          if (rowKey != null) {
            if (rowKey.equals(tmpKey)) {
              merge = true;
            }
          }

          if (merge) {
            if (deleteFamily) {
              fillCellDeleteFamily(row, hbaseMeta);
            } else {
              fillCell(row, cell, hbaseMeta, delete);
            }
          } else {
            if (row != null) {
              carbonWriter.write(row);
            }
            row = new String[hbaseMeta.getSchema().getFieldsLength()];
            int[] keyColumnIndexes = hbaseMeta.getKeyColumnIndex();

            // should decode
            HBaseTableReference tableReference = HBaseTableReferenceBuilder
                .buildTableReference("replicate", hbaseMeta.getPrimaryKeyColumns(),
                    hbaseMeta.getSchema().getFields());
            converter.convertRowKeyWithDecode(tableReference, keyColumnIndexes.length,
                cell.getRowArray(), cell.getRowOffset(), cell.getRowLength(), keyColumnIndexes,
                hbaseMeta.getSchema().getFields(), row);

            row[hbaseMeta.getTimestampMapIndex()] = String.valueOf(cell.getTimestamp());
            if (deleteFamily) {
              fillCellDeleteFamily(row, hbaseMeta);
            } else {
              fillCell(row, cell, hbaseMeta, delete);
            }

            if (delete) {
              row[hbaseMeta.getDeleteStatusMap()] = DELETE;
            } else if (deleteFamily) {
              row[hbaseMeta.getDeleteStatusMap()] = DELETEFAMILY;
            } else {
              row[hbaseMeta.getDeleteStatusMap()] = INSERT;
            }
          }
          rowKey = tmpKey;
        }
      }
      if (row != null) {
        carbonWriter.write(row);
      }
      carbonWriter.flushBatch();
    } catch (Exception e) {
      LOG.error("Exception occured while performing writer flush", e);
    }
  }

  private static void fillCellDeleteFamily(String[] row, CarbonHbaseMeta meta) {

    for (Integer index : meta.getSchemaMapping().values()) {
      row[index] = DELETE;
    }
  }

  private static void fillCell(String[] row, Cell cell, CarbonHbaseMeta meta, boolean delete) {

    int index = meta.getSchemaIndexOfColumn(cell.getFamilyArray(), cell.getFamilyOffset(),
        cell.getFamilyLength(), cell.getQualifierArray(), cell.getQualifierOffset(),
        cell.getQualifierLength());
    if (index == -1) {
      return;
    }
    if (!delete) {

      // should decode
      row[index] = meta.getDataTypeConverter()
          .convertWithDecode(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength(),
              meta.getSchema().getFields()[index].getDataType());
    } else {
      row[index] = DELETE;
    }
  }

  private void loadCarbonConfigFromFile(Configuration conf) {
    String endPoint = conf.get(SparkS3Constants.FS_OBS_END_POINT);
    String ak = conf.get(SparkS3Constants.FS_OBS_AK);
    String sk = conf.get(SparkS3Constants.FS_OBS_SK);
    String carbonConfigPath = conf.get(CARBON_CONFIG_FOLDER);
    if (endPoint != null && ak != null && sk != null && carbonConfigPath != null) {
      CarbonPropertiesLoader.loadConfig(endPoint, ak, sk, carbonConfigPath);
    } else {
      LOG.warn("Carbon config file info is not found, will not load it.");
    }
  }
}
