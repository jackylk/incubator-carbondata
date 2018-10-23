/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.carbondata.hadoop;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.PartitionSpec;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapDistributable;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.BlockletDataMapDetailsWithSchema;
import org.apache.carbondata.core.util.CarbonBlockLoaderHelper;

import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.log4j.Logger;

/**
 * class to load blocklet data map
 */
public class DistributableBlockletDataMapLoader
    extends FileInputFormat<TableBlockIndexUniqueIdentifier, BlockletDataMapDetailsWithSchema>
    implements Serializable {

  /**
   * Attribute for Carbon LOGGER.
   */
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(DistributableBlockletDataMapLoader.class.getName());

  private static final long serialVersionUID = 1;

  private CarbonTable table;

  private DataMapExprWrapper dataMapExprWrapper;

  private List<Segment> validSegments;

  private Set<String> keys;

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
    DataMapFactory dataMapFactory =
        DataMapStoreManager.getInstance().getDefaultDataMap(table).getDataMapFactory();
    CacheableDataMap factory = (CacheableDataMap) dataMapFactory;
    List<DataMapDistributable> validDistributables =
        factory.getAllUncachedDistributables(distributableList);
    CarbonBlockLoaderHelper instance = CarbonBlockLoaderHelper.getInstance();
    int distributableSize = validDistributables.size();
    List<InputSplit> inputSplits = new ArrayList<>(distributableSize);
    keys = new HashSet<>();
    Iterator<DataMapDistributable> iterator = validDistributables.iterator();
    while (iterator.hasNext()) {
      BlockletDataMapDistributable next = (BlockletDataMapDistributable) iterator.next();
      String key = next.getFilePath();
      if (instance.checkAlreadySubmittedBlock(table.getAbsoluteTableIdentifier(), key)) {
        inputSplits.add(next);
        keys.add(key);
      }
    }
    int sizeOfDistToBeLoaded = inputSplits.size();
    LOGGER.info("Submitted blocks " + sizeOfDistToBeLoaded + ", " + distributableSize
        + " . Rest already considered for load in other job.");
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
            new BlockletDataMapDetailsWithSchema(wrapper, table.getTableInfo().isSchemaModified());
        return blockletDataMapDetailsWithSchema;
      }

      @Override public float getProgress() throws IOException, InterruptedException {
        return 0;
      }

      @Override public void close() throws IOException {
        if (null != tableBlockIndexUniqueIdentifierWrapper) {
          if (null != wrapper && null != wrapper.getDataMaps() && !wrapper.getDataMaps()
              .isEmpty()) {
            String segmentId =
                tableBlockIndexUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier()
                    .getSegmentId();
            // as segmentId will be same for all the dataMaps and segmentProperties cache is
            // maintained at segment level so it need to be called only once for clearing
            SegmentPropertiesAndSchemaHolder.getInstance()
                .invalidate(segmentId, wrapper.getDataMaps().get(0).getSegmentPropertiesIndex(),
                    tableBlockIndexUniqueIdentifierWrapper.isAddTableBlockToUnsafeAndLRUCache());
          }
        }
      }

    };
  }

  public void invalidate() {
    if (null != keys) {
      CarbonBlockLoaderHelper instance = CarbonBlockLoaderHelper.getInstance();
      instance.clear(table.getAbsoluteTableIdentifier(), keys);
    }
  }
}
