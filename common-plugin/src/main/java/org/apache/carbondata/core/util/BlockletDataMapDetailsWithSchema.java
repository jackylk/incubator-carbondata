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
package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;

/**
 * class that holds dataMaps, column cardinality, columnSchema and other related information for
 * DistributableBlockletDataMapLoader return value
 * TODO: When this code is moved to open source, this class can be removed and the required code
 * can be added to BlockletDataMapIndexWrapper class
 */
public class BlockletDataMapDetailsWithSchema implements Serializable {

  private static final long serialVersionUID = 8879439848531370730L;

  private BlockletDataMapIndexWrapper blockletDataMapIndexWrapper;

  private Map<Integer, ColumnSchemaWrapper> dataMapIndexToColumnSchemaMapping = new HashMap<>();

  public BlockletDataMapDetailsWithSchema(
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper) {
    this.blockletDataMapIndexWrapper = blockletDataMapIndexWrapper;
    List<BlockDataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
    for (BlockDataMap blockDataMap : dataMaps) {
      int segmentPropertiesIndex = blockDataMap.getSegmentPropertiesIndex();
      if (null == dataMapIndexToColumnSchemaMapping.get(segmentPropertiesIndex)) {
        SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper segmentPropertiesWrapper =
            SegmentPropertiesAndSchemaHolder.getInstance()
                .getSegmentPropertiesWrapper(segmentPropertiesIndex);
        // create new Column Schema wrapper instance
        ColumnSchemaWrapper columnSchemaWrapper =
            new ColumnSchemaWrapper(segmentPropertiesWrapper.getColumnsInTable(),
                segmentPropertiesWrapper.getColumnCardinality());
        // add new mapping entry to the map
        dataMapIndexToColumnSchemaMapping.put(segmentPropertiesIndex, columnSchemaWrapper);
      }
    }
  }

  public BlockletDataMapIndexWrapper getBlockletDataMapIndexWrapper() {
    return blockletDataMapIndexWrapper;
  }

  private List<ColumnSchema> getColumnSchemaList(int index) {
    return dataMapIndexToColumnSchemaMapping.get(index).getColumnSchemaList();
  }

  private int[] getColumnCardinality(int index) {
    return dataMapIndexToColumnSchemaMapping.get(index).getColumnCardinality();
  }

  /**
   * add segmentProperties in the segment properties holder instance
   *
   * @param carbonTable
   * @param segmentId
   * @param index
   */
  public int addSegmentProperties(CarbonTable carbonTable, String segmentId, int index) {
    return SegmentPropertiesAndSchemaHolder.getInstance()
        .addSegmentProperties(carbonTable, getColumnSchemaList(index), getColumnCardinality(index),
            segmentId);
  }

  public static class ColumnSchemaWrapper implements Serializable {

    private static final long serialVersionUID = 1329083135274487751L;
    private List<ColumnSchema> columnSchemaList;

    private int[] columnCardinality;

    public ColumnSchemaWrapper(List<ColumnSchema> columnSchemaList, int[] columnCardinality) {
      this.columnSchemaList = columnSchemaList;
      this.columnCardinality = columnCardinality;
    }

    public List<ColumnSchema> getColumnSchemaList() {
      return columnSchemaList;
    }

    public int[] getColumnCardinality() {
      return columnCardinality;
    }

  }
}
