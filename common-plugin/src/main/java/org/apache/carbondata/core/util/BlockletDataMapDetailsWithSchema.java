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
package org.apache.carbondata.core.util;

import java.io.Serializable;
import java.util.List;

import org.apache.carbondata.core.datastore.block.SegmentPropertiesAndSchemaHolder;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockDataMap;
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

  private int[] columnCardinality;

  private List<ColumnSchema> columnSchemaList;

  public BlockletDataMapDetailsWithSchema(
      BlockletDataMapIndexWrapper blockletDataMapIndexWrapper, boolean isSchemaModified) {
    this.blockletDataMapIndexWrapper = blockletDataMapIndexWrapper;
    List<BlockDataMap> dataMaps = blockletDataMapIndexWrapper.getDataMaps();
    if (!dataMaps.isEmpty()) {
      // In one task all dataMaps will have the same cardinality and schema therefore
      // segmentPropertyIndex can be fetched from one dataMap
      SegmentPropertiesAndSchemaHolder.SegmentPropertiesWrapper
          segmentPropertiesWrapper = dataMaps.get(0).getSegmentPropertiesWrapper();
      columnCardinality = segmentPropertiesWrapper.getColumnCardinality();
      // flag to check whether carbon table schema is modified. ColumnSchemaList will be
      // serialized from executor to driver only if schema is modified
      if (isSchemaModified) {
        columnSchemaList = segmentPropertiesWrapper.getColumnsInTable();
      }
    }
  }

  public BlockletDataMapIndexWrapper getBlockletDataMapIndexWrapper() {
    return blockletDataMapIndexWrapper;
  }

  public List<ColumnSchema> getColumnSchemaList() {
    return columnSchemaList;
  }

  public int[] getColumnCardinality() {
    return columnCardinality;
  }
}
