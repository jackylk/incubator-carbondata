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

package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.Cache;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.datamap.DataMapDistributable;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.dev.CacheableDataMap;
import org.apache.carbondata.core.datamap.dev.DataMapFactory;
import org.apache.carbondata.core.datamap.dev.expr.DataMapDistributableWrapper;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.util.BlockletDataMapDetailsWithSchema;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

/**
 * class to load blocklet data map
 */
public class DistributableBlockletDataMapLoader
    extends FileInputFormat<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>
    implements Serializable {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final LogService LOGGER =
      LogServiceFactory.getLogService(DistributableBlockletDataMapLoader.class.getName());

  private static final long serialVersionUID = 1;

  private CarbonTable table;

  private DataMapExprWrapper dataMapExprWrapper;

  private List<Segment> validSegments;

  public DistributableBlockletDataMapLoader(CarbonTable table,
      DataMapExprWrapper dataMapExprWrapper, List<Segment> validSegments,
      List<Segment> invalidSegments, List<PartitionSpec> partitions, boolean isJobToClearDataMaps) {
    this.table = table;
    this.dataMapExprWrapper = dataMapExprWrapper;
    this.validSegments = validSegments;
  }

  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    List<DataMapDistributableWrapper> distributables =
        dataMapExprWrapper.toDistributable(validSegments);
    List<DataMapDistributable> distributableList = new ArrayList<>();
    for (DataMapDistributableWrapper wrapper : distributables) {
      distributableList.add(wrapper.getDistributable());
    }
    List<DataMapDistributable> validDistributables = new ArrayList<>();
    try {
      DataMapSchema dataMapSchema = BlockletDataMapFactory.DATA_MAP_SCHEMA;
      DataMapFactory dataMapFactory = DataMapStoreManager.getInstance()
          .getDataMapFactoryClass(table, dataMapSchema);
      CacheableDataMap factory = (CacheableDataMap) dataMapFactory;
      validDistributables = factory.getAllUncachedDistributables(distributableList);
    } catch (MalformedDataMapCommandException e) {
      LOGGER.error(e, "failed to get DataMap factory for the provided schema");
    }
    List<InputSplit> inputSplits = new ArrayList<>(validDistributables.size());
    inputSplits.addAll(validDistributables);
    return inputSplits;
  }

  @Override
  public RecordReader<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>
      createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    return new RecordReader<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>() {
      private BlockletDataMapIndexWrapper wrapper = null;
      private TableBlockIndexUniqueIdentifier tableBlockIndexUniqueIdentifier = null;
      private TableBlockIndexUniqueIdentifierWrapper tableBlockIndexUniqueIdentifierWrapper;
      Cache<TableBlockIndexUniqueIdentifierWrapper, BlockletDataMapIndexWrapper> cache =
          CacheProvider.getInstance().createCache(CacheType.DRIVER_BLOCKLET_DATAMAP);
      boolean finished = false;

      @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
          throws IOException, InterruptedException {
        BlockletDataMapDistributable blockletDistributable =
            (BlockletDataMapDistributable) inputSplit;
        tableBlockIndexUniqueIdentifier =
            blockletDistributable.getTableBlockIndexUniqueIdentifier();
        tableBlockIndexUniqueIdentifierWrapper =
            new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier, table,
                false);
        wrapper = cache.get(tableBlockIndexUniqueIdentifierWrapper);
      }

      @Override public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!finished) {
          finished = true;
          return finished;
        } else {
          return false;
        }
      }

      @Override public TableBlockIndexUniqueIdentifier getCurrentKey()
          throws IOException, InterruptedException {
        return tableBlockIndexUniqueIdentifier;
      }

      @Override public BlockletDataMapDetailsWithSchema getCurrentValue()
          throws IOException, InterruptedException {
        BlockletDataMapDetailsWithSchema blockletDataMapDetailsWithSchema =
            new BlockletDataMapDetailsWithSchema(wrapper);
        return blockletDataMapDetailsWithSchema;
      }

      @Override public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override public void close() throws IOException {
        if (null != tableBlockIndexUniqueIdentifierWrapper) {
          cache.invalidate(tableBlockIndexUniqueIdentifierWrapper);
        }
      }

    };
  }
}
