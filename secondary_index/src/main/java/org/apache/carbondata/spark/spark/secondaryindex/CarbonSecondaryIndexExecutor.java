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
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.model.QueryModelBuilder;
import org.apache.carbondata.core.scan.result.RowBatch;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverter;

import org.apache.log4j.Logger;

/**
 * Executor class for executing the query on every segment for creating secondary index.
 * This will fire a select query on index columns and get the result.
 */
public class CarbonSecondaryIndexExecutor {

  private TaskBlockInfo taskBlockInfo;
  /**
   * List of columns on which secondary index need to be created
   */
  private String[] secondaryIndexColumns;
  private QueryExecutor queryExecutor;
  private CarbonTable carbonTable;
  private QueryModel queryModel;
  // converter for UTF8String and decimal conversion
  private DataTypeConverter dataTypeConverter;

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(CarbonSecondaryIndexExecutor.class.getName());

  /**
   * Constructor
   *
   * @param taskBlockInfo
   * @param carbonTable
   * @param secondaryIndexColumns
   */
  public CarbonSecondaryIndexExecutor(TaskBlockInfo taskBlockInfo, CarbonTable carbonTable,
      List<String> secondaryIndexColumns, DataTypeConverter dataTypeConverter) {
    this.taskBlockInfo = taskBlockInfo;
    this.secondaryIndexColumns = new String[secondaryIndexColumns.size()];
    secondaryIndexColumns.toArray(this.secondaryIndexColumns);
    this.carbonTable = carbonTable;
    this.dataTypeConverter = dataTypeConverter;

  }

  /**
   * For processing of the table blocks.
   *
   * @return List of Carbon iterators
   */
  public List<CarbonIterator<RowBatch>> processTableBlocks() throws QueryExecutionException {
    List<CarbonIterator<RowBatch>> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<TableBlockInfo> blockList = null;
    queryModel = prepareQueryModel();
    this.queryExecutor =
        QueryExecutorFactory.getQueryExecutor(queryModel, FileFactory.getConfiguration());
    // for each segment get task block info
    Set<String> taskBlockListMapping = taskBlockInfo.getTaskSet();
    for (String task : taskBlockListMapping) {
      blockList = taskBlockInfo.getTableBlockInfoList(task);
      Collections.sort(blockList);
      LOGGER.info("for task -" + task + "-block size is -" + blockList.size());
      queryModel.setTableBlockInfos(blockList);
      resultList.add(executeBlockList(blockList));
    }
    return resultList;
  }

  /**
   * Below method will be used
   * for cleanup
   */
  public void finish() {
    try {
      queryExecutor.finish();
    } catch (QueryExecutionException e) {
      LOGGER.error("Problem while finish: ", e);
    }
    clearDictionaryFromQueryModel();
  }

  /**
   * get executor and execute the query model.
   *
   * @param blockList
   * @return
   */
  private CarbonIterator<RowBatch> executeBlockList(List<TableBlockInfo> blockList)
      throws QueryExecutionException {
    queryModel.setTableBlockInfos(blockList);
    CarbonIterator<RowBatch> iter = null;
    try {
      iter = queryExecutor.execute(queryModel);
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
      throw new QueryExecutionException(e);
    }
    return iter;
  }

  /**
   * This method will clear the dictionary access count after its usage is complete so
   * that column can be deleted form LRU cache whenever memory reaches threshold
   */
  public void clearDictionaryFromQueryModel() {
    if (null != queryModel) {
      Map<String, Dictionary> columnToDictionaryMapping = queryModel.getColumnToDictionaryMapping();
      if (null != columnToDictionaryMapping) {
        for (Map.Entry<String, Dictionary> entry : columnToDictionaryMapping.entrySet()) {
          CarbonUtil.clearDictionaryCache(entry.getValue());
        }
      }
    }
  }

  /**
   * Preparing the query model.
   *
   * @return
   */
  public QueryModel prepareQueryModel() {

    // Add implicit column position id or row id in case of secondary index creation
    List<CarbonDimension> implicitDimensionList =
        carbonTable.getImplicitDimensionByTableName(carbonTable.getTableName());
    String[] columnsArray = new String[implicitDimensionList.size() + secondaryIndexColumns.length];
    int j = 0;
    for (int i = 0; i < secondaryIndexColumns.length; i++) {
      columnsArray[j] = secondaryIndexColumns[i];
      j++;
    }
    for (int i = 0; i < implicitDimensionList.size(); i++) {
      columnsArray[j] = implicitDimensionList.get(i).getColName();
      j++;
    }
    QueryModelBuilder builder = new QueryModelBuilder(carbonTable).projectColumns(columnsArray)
        .dataConverter(dataTypeConverter).enableForcedDetailRawQuery();
    QueryModel model = builder.build();
    model.setQueryId(System.nanoTime() + "");
    return model;
  }
}
