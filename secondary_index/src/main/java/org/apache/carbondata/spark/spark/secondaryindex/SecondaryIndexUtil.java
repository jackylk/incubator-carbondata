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
package org.apache.carbondata.spark.spark.secondaryindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.blocklet.DataFileFooter;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.spark.spark.secondaryindex.exception.SecondaryIndexException;

/**
 * Utility Class for the Secondary Index creation flow
 */
public class SecondaryIndexUtil {

  private static final LogService LOG =
      LogServiceFactory.getLogService(SecondaryIndexUtil.class.getName());

  /**
   * To create a mapping of task and block
   *
   * @param tableBlockInfoList
   * @return
   */
  public static TaskBlockInfo createTaskAndBlockMapping(List<TableBlockInfo> tableBlockInfoList) {
    TaskBlockInfo taskBlockInfo = new TaskBlockInfo();
    for (TableBlockInfo info : tableBlockInfoList) {
      // extract task ID from file Path.
      String taskNo = CarbonTablePath.DataFileUtil.getTaskNo(info.getFilePath());
      groupCorrespodingInfoBasedOnTask(info, taskBlockInfo, taskNo);
    }
    return taskBlockInfo;
  }

  /**
   * Grouping the taskNumber and list of TableBlockInfo.
   *
   * @param info
   * @param taskBlockMapping
   * @param taskNo
   */
  private static void groupCorrespodingInfoBasedOnTask(TableBlockInfo info,
      TaskBlockInfo taskBlockMapping, String taskNo) {
    // get the corresponding list from task mapping.
    List<TableBlockInfo> blockLists = taskBlockMapping.getTableBlockInfoList(taskNo);
    if (null != blockLists) {
      blockLists.add(info);
    } else {
      blockLists = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
      blockLists.add(info);
      taskBlockMapping.addTableBlockInfoList(taskNo, blockLists);
    }
  }

  /**
   * This method will read the file footer of given block
   *
   * @param tableBlockInfo
   * @return
   * @throws SecondaryIndexException
   */
  public static DataFileFooter readFileFooter(TableBlockInfo tableBlockInfo)
      throws SecondaryIndexException {
    DataFileFooter dataFileFooter = null;
    try {
      dataFileFooter = CarbonUtil
          .readMetadatFile(tableBlockInfo);
    } catch (IOException e) {
      throw new SecondaryIndexException(
          "Problem reading the file footer during secondary index creation: " + e.getMessage());
    }
    return dataFileFooter;
  }

  /**
   * This method will iterate over dimensions of fact table and prepare the
   * column cardinality for index table
   *
   * @param dataFileFooter
   * @param columnCardinalityForFactTable
   * @param databaseName
   * @param factTableName
   * @param indexTableName
   * @return
   */
  public static int[] prepareColumnCardinalityForIndexTable(DataFileFooter dataFileFooter,
      int[] columnCardinalityForFactTable, String databaseName, String factTableName,
      String indexTableName) {
    int[] columnCardinalityForIndexTable = null;
    List<CarbonDimension> factTableDimensions = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + factTableName)
        .getDimensionByTableName(factTableName);
    List<CarbonDimension> indexTableDimensions = CarbonMetadata.getInstance()
        .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + indexTableName)
        .getDimensionByTableName(indexTableName);
    List<Integer> factToIndexTableDimensionIndexMapping =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (CarbonDimension indexTableDimension : indexTableDimensions) {
      int columnIndex = 0;
      for (int i = 0; i < factTableDimensions.size(); i++) {
        // increment the dictionary index only so that matching dimension index is properly
        // added to list for getting the column cardinality
        if (factTableDimensions.get(i).getColumnId().equals(indexTableDimension.getColumnId())) {
          factToIndexTableDimensionIndexMapping.add(columnIndex);
          break;
        }
        columnIndex++;
      }
    }
    if (factToIndexTableDimensionIndexMapping.isEmpty()) {
      columnCardinalityForIndexTable = new int[0];
    } else {
      columnCardinalityForIndexTable = new int[factToIndexTableDimensionIndexMapping.size()];
      for (int i = 0; i < factToIndexTableDimensionIndexMapping.size(); i++) {
        columnCardinalityForIndexTable[i] =
            columnCardinalityForFactTable[factToIndexTableDimensionIndexMapping.get(i)];
      }
    }
    return columnCardinalityForIndexTable;
  }

  /**
   * This method will return fact table to index table column mapping
   *
   * @param databaseName
   * @param factTableName
   * @param indexTableName
   * @return
   */
  public static int[] prepareColumnMappingOfFactToIndexTable(
      String databaseName, String factTableName,
      String indexTableName, Boolean isDictColsAlone) {
    List<CarbonDimension> factTableDimensions = CarbonMetadata.getInstance()
            .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + factTableName)
            .getDimensionByTableName(factTableName);
    List<CarbonDimension> indexTableDimensions = CarbonMetadata.getInstance()
            .getCarbonTable(databaseName + CarbonCommonConstants.UNDERSCORE + indexTableName)
            .getDimensionByTableName(indexTableName);
    List<Integer> dims = new ArrayList<Integer>();
    for (CarbonDimension indexTableDimension : indexTableDimensions) {
      for (int i = 0; i < factTableDimensions.size(); i++) {
        CarbonDimension dim = factTableDimensions.get(i);
        if (dim.getColumnId().equals(indexTableDimension.getColumnId())) {
          if (isDictColsAlone && dim.hasEncoding(Encoding.DICTIONARY)) {
            dims.add(i);
          } else if (!isDictColsAlone) {
            dims.add(i);
          }
          break;
        }
      }
    }
    List<Integer> sortedDims = new ArrayList<Integer>(dims.size());
    sortedDims.addAll(dims);
    Collections.sort(sortedDims);
    int dimsCount = sortedDims.size();
    int[] indexToFactColMapping = new int[dimsCount];
    for (int i = 0; i < dimsCount; i++) {
      indexToFactColMapping[sortedDims.indexOf(dims.get(i))] = i;
    }
    return indexToFactColMapping;
  }

  /**
   * This method will update the deletion status for all the index tables
   *
   * @param parentCarbonTable
   * @param indexTables
   * @throws IOException
   */
  public static void updateTableStatusForIndexTables(CarbonTable parentCarbonTable,
      List<CarbonTable> indexTables) throws IOException {

    LoadMetadataDetails[] loadFolderDetailsArrayMainTable =
        SegmentStatusManager.readLoadMetadata(parentCarbonTable.getMetadataPath());
    for (CarbonTable indexTable : indexTables) {

      String tableStatusFilePath =
          CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath());
      if (!CarbonUtil.isFileExists(tableStatusFilePath)) {
        LOG.info(
            "Table status file does not exist for index table: " + indexTable.getTableUniqueName());
        continue;
      }
      LoadMetadataDetails[] loadFolderDetailsArray =
          SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath());
      if (null != loadFolderDetailsArray && loadFolderDetailsArray.length > 0) {
        List<String> invalidLoads = new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        try {
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath()),
              loadFolderDetailsArrayMainTable);
          if (invalidLoads.size() > 0) {
            LOG.audit("Delete segment by Id is successfull for $dbName.$tableName.");
          } else {
            LOG.error("Delete segment by Id is failed. Invalid ID is: " + invalidLoads.toString());
          }
        } catch (Exception ex) {
          LOG.error(ex.getMessage());
        }
      }
    }
  }
}
