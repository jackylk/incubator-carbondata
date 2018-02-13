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

package org.apache.carbondata.spark.spark.secondaryindex;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.dictionary.Dictionary;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.block.TableBlockInfo;
import org.apache.carbondata.core.datastore.block.TaskBlockInfo;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.scan.executor.QueryExecutor;
import org.apache.carbondata.core.scan.executor.QueryExecutorFactory;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.model.QueryDimension;
import org.apache.carbondata.core.scan.model.QueryMeasure;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.scan.result.BatchResult;
import org.apache.carbondata.core.util.CarbonUtil;

/**
 * Executor class for executing the query on every segment for creating secondary index.
 * This will fire a select query on index columns and get the result.
 */
public class CarbonSecondaryIndexExecutor {

  private TaskBlockInfo taskBlockInfo;
  /**
   * List of columns on which secondary index need to be created
   */
  private List<String> secondaryIndexColumns;
  private QueryExecutor queryExecutor;
  private CarbonTable carbonTable;
  private QueryModel queryModel;

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonSecondaryIndexExecutor.class.getName());

  /**
   * Constructor
   *
   * @param taskBlockInfo
   * @param carbonTable
   * @param secondaryIndexColumns
   */
  public CarbonSecondaryIndexExecutor(TaskBlockInfo taskBlockInfo, CarbonTable carbonTable,
      List<String> secondaryIndexColumns) {
    this.taskBlockInfo = taskBlockInfo;
    this.secondaryIndexColumns = secondaryIndexColumns;
    this.carbonTable = carbonTable;
  }

  /**
   * For processing of the table blocks.
   *
   * @return List of Carbon iterators
   */
  public List<CarbonIterator<BatchResult>> processTableBlocks() throws QueryExecutionException {
    List<CarbonIterator<BatchResult>> resultList =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<TableBlockInfo> blockList = null;
    queryModel = prepareQueryModel();
    this.queryExecutor = QueryExecutorFactory.getQueryExecutor(queryModel);
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
      LOGGER.error(e, "Problem while finish: ");
    }
    clearDictionaryFromQueryModel();
  }

  /**
   * get executor and execute the query model.
   *
   * @param blockList
   * @return
   */
  private CarbonIterator<BatchResult> executeBlockList(List<TableBlockInfo> blockList)
      throws QueryExecutionException {
    queryModel.setTableBlockInfos(blockList);
    CarbonIterator<BatchResult> iter = null;
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
    QueryModel model = new QueryModel();
    model.setForcedDetailRawQuery(true);
    model.setFilterExpressionResolverTree(null);
    List<QueryDimension> queryDimensions =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    List<QueryMeasure> queryMeasures =
        new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (String columnName : secondaryIndexColumns) {
      QueryDimension queryDimension = new QueryDimension(columnName);
      queryDimensions.add(queryDimension);
    }
    // Add implicit column position id or row id in case of secondary index creation
    List<CarbonDimension> implicitDimensionList =
        carbonTable.getImplicitDimensionByTableName(carbonTable.getTableName());
    for (CarbonDimension dimension : implicitDimensionList) {
      QueryDimension queryDimension = new QueryDimension(dimension.getColName());
      queryDimensions.add(queryDimension);
    }
    model.setQueryDimension(queryDimensions);
    model.setQueryMeasures(queryMeasures);
    model.setQueryId(System.nanoTime() + "");
    model.setAbsoluteTableIdentifier(carbonTable.getAbsoluteTableIdentifier());
    model.setTable(carbonTable);
    return model;
  }

}