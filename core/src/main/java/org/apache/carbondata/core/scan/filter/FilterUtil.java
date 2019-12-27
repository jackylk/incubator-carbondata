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

package org.apache.carbondata.core.scan.filter;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.cache.CacheProvider;
import org.apache.carbondata.core.cache.CacheType;
import org.apache.carbondata.core.cache.dictionary.DictionaryColumnUniqueIdentifier;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.constants.CarbonV3DataFormatConstants;
import org.apache.carbondata.core.datastore.IndexKey;
import org.apache.carbondata.core.datastore.block.SegmentProperties;
import org.apache.carbondata.core.datastore.chunk.DimensionColumnPage;
import org.apache.carbondata.core.datastore.chunk.impl.DimensionRawColumnChunk;
import org.apache.carbondata.core.keygenerator.KeyGenException;
import org.apache.carbondata.core.keygenerator.KeyGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryGenerator;
import org.apache.carbondata.core.keygenerator.directdictionary.DirectDictionaryKeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.factory.KeyGeneratorFactory;
import org.apache.carbondata.core.keygenerator.mdkey.MultiDimKeyVarLengthGenerator;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnIdentifier;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.encoder.Encoding;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonDimension;
import org.apache.carbondata.core.metadata.schema.table.column.CarbonMeasure;
import org.apache.carbondata.core.scan.executor.exception.QueryExecutionException;
import org.apache.carbondata.core.scan.executor.util.QueryUtil;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.ExpressionResult;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.ConditionalExpression;
import org.apache.carbondata.core.scan.expression.conditional.ImplicitExpression;
import org.apache.carbondata.core.scan.expression.conditional.InExpression;
import org.apache.carbondata.core.scan.expression.conditional.ListExpression;
import org.apache.carbondata.core.scan.expression.exception.FilterIllegalMemberException;
import org.apache.carbondata.core.scan.expression.exception.FilterUnsupportedException;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.expression.logical.TrueExpression;
import org.apache.carbondata.core.scan.filter.executer.AndFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.DimColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.ExcludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.FalseFilterExecutor;
import org.apache.carbondata.core.scan.filter.executer.FilterExecuter;
import org.apache.carbondata.core.scan.filter.executer.ImplicitIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.IncludeFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.MeasureColumnExecuterFilterInfo;
import org.apache.carbondata.core.scan.filter.executer.OrFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RangeValueFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureExcludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RestructureIncludeFilterExecutorImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelFilterExecuterImpl;
import org.apache.carbondata.core.scan.filter.executer.RowLevelRangeTypeExecuterFactory;
import org.apache.carbondata.core.scan.filter.executer.TrueFilterExecutor;
import org.apache.carbondata.core.scan.filter.intf.ExpressionType;
import org.apache.carbondata.core.scan.filter.intf.FilterExecuterType;
import org.apache.carbondata.core.scan.filter.intf.RowImpl;
import org.apache.carbondata.core.scan.filter.intf.RowIntf;
import org.apache.carbondata.core.scan.filter.resolver.ConditionalFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.RowLevelRangeFilterResolverImpl;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.ColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.DimColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.filter.resolver.resolverinfo.MeasureColumnResolvedFilterInfo;
import org.apache.carbondata.core.scan.result.vector.CarbonDictionary;
import org.apache.carbondata.core.util.BitSetGroup;
import org.apache.carbondata.core.util.ByteUtil;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.DataTypeConverterImpl;
import org.apache.carbondata.core.util.DataTypeUtil;
import org.apache.carbondata.core.util.comparator.Comparator;
import org.apache.carbondata.core.util.comparator.SerializableComparator;

import org.apache.commons.lang.ArrayUtils;
import org.apache.log4j.Logger;
import org.roaringbitmap.RoaringBitmap;

public final class FilterUtil {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(FilterUtil.class.getName());

  private FilterUtil() {

  }

  /**
   * Pattern used : Visitor Pattern
   * Method will create filter executer tree based on the filter resolved tree,
   * in this algorithm based on the resolver instance the executers will be visited
   * and the resolved surrogates will be converted to keys
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @param complexDimensionInfoMap
   * @param minMaxCacheColumns
   * @param isStreamDataFile: whether create filter executer tree for stream data files
   * @return FilterExecuter instance
   *
   */
  private static FilterExecuter createFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    FilterExecuterType filterExecuterType = filterExpressionResolverTree.getFilterExecuterType();
    if (null != filterExecuterType) {
      switch (filterExecuterType) {
        case INCLUDE:
          if (null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              && null != filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues() && filterExpressionResolverTree.getDimColResolvedFilterInfo()
              .getFilterValues().isOptimized()) {
            return getExcludeFilterExecuter(
                filterExpressionResolverTree.getDimColResolvedFilterInfo(),
                filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
          }
          // return true filter expression if filter column min/max is not cached in driver
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return getIncludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case EXCLUDE:
          return getExcludeFilterExecuter(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getMsrColResolvedFilterInfo(), segmentProperties);
        case OR:
          return new OrFilterExecuterImpl(
              createFilterExecuterTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile));
        case AND:
          return new AndFilterExecuterImpl(
              createFilterExecuterTree(filterExpressionResolverTree.getLeft(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile),
              createFilterExecuterTree(filterExpressionResolverTree.getRight(), segmentProperties,
                  complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile));
        case ROWLEVEL_LESSTHAN:
        case ROWLEVEL_LESSTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN_EQUALTO:
        case ROWLEVEL_GREATERTHAN:
          // return true filter expression if filter column min/max is not cached in driver
          RowLevelRangeFilterResolverImpl rowLevelRangeFilterResolver =
              (RowLevelRangeFilterResolverImpl) filterExpressionResolverTree;
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
              rowLevelRangeFilterResolver.getDimColEvaluatorInfoList(),
              rowLevelRangeFilterResolver.getMsrColEvalutorInfoList(), segmentProperties,
              minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return RowLevelRangeTypeExecuterFactory
              .getRowLevelRangeTypeExecuter(filterExecuterType, filterExpressionResolverTree,
                  segmentProperties);
        case RANGE:
          // return true filter expression if filter column min/max is not cached in driver
          if (checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(filterExpressionResolverTree,
              segmentProperties, minMaxCacheColumns, isStreamDataFile)) {
            return new TrueFilterExecutor();
          }
          return new RangeValueFilterExecuterImpl(
              filterExpressionResolverTree.getDimColResolvedFilterInfo(),
              filterExpressionResolverTree.getFilterExpression(),
              ((ConditionalFilterResolverImpl) filterExpressionResolverTree)
                  .getFilterRangeValues(segmentProperties), segmentProperties);
        case TRUE:
          return new TrueFilterExecutor();
        case FALSE:
          return new FalseFilterExecutor();
        case ROWLEVEL:
        default:
          return new RowLevelFilterExecuterImpl(
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getDimColEvaluatorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree)
                  .getMsrColEvalutorInfoList(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
              ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
              segmentProperties, complexDimensionInfoMap);

      }
    }
    return new RowLevelFilterExecuterImpl(
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getDimColEvaluatorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getMsrColEvalutorInfoList(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getFilterExpresion(),
        ((RowLevelFilterResolverImpl) filterExpressionResolverTree).getTableIdentifier(),
        segmentProperties, complexDimensionInfoMap);

  }

  /**
   * It gives filter executer based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecuter getIncludeFilterExecuter(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      MeasureColumnResolvedFilterInfo msrColResolvedFilterInfo,
      SegmentProperties segmentProperties) {
    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isMeasure()) {
      CarbonMeasure measuresFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure());
      if (null != measuresFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        MeasureColumnResolvedFilterInfo msrColResolvedFilterInfoCopyObject =
            msrColResolvedFilterInfo.getCopyObject();
        msrColResolvedFilterInfoCopyObject.setMeasure(measuresFromCurrentBlock);
        msrColResolvedFilterInfoCopyObject.setColumnIndex(measuresFromCurrentBlock.getOrdinal());
        msrColResolvedFilterInfoCopyObject.setType(measuresFromCurrentBlock.getDataType());
        return new IncludeFilterExecuterImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureIncludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
    CarbonDimension dimension = dimColResolvedFilterInfo.getDimension();
    if (dimension.hasEncoding(Encoding.IMPLICIT)) {
      return new ImplicitIncludeFilterExecutorImpl(dimColResolvedFilterInfo);
    } else {
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(dimColResolvedFilterInfo.getDimension());
      if (null != dimensionFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        DimColumnResolvedFilterInfo dimColResolvedFilterInfoCopyObject =
            dimColResolvedFilterInfo.getCopyObject();
        dimColResolvedFilterInfoCopyObject.setDimension(dimensionFromCurrentBlock);
        dimColResolvedFilterInfoCopyObject.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
        return new IncludeFilterExecuterImpl(dimColResolvedFilterInfoCopyObject, null,
            segmentProperties, false);
      } else {
        return new RestructureIncludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, false);
      }
    }
  }

  /**
   * check if current need to be replaced with TrueFilter expression. This will happen in case
   * filter column min/max is not cached in the driver
   *
   * @param dimColEvaluatorInfoList
   * @param msrColEvaluatorInfoList
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @return
   */
  private static boolean checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
      List<DimColumnResolvedFilterInfo> dimColEvaluatorInfoList,
      List<MeasureColumnResolvedFilterInfo> msrColEvaluatorInfoList,
      SegmentProperties segmentProperties, List<CarbonColumn> minMaxCacheColumns,
      boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    ColumnResolvedFilterInfo columnResolvedFilterInfo = null;
    if (!msrColEvaluatorInfoList.isEmpty()) {
      columnResolvedFilterInfo = msrColEvaluatorInfoList.get(0);
      replaceCurrentNodeWithTrueFilter =
          checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
              minMaxCacheColumns, true, isStreamDataFile);
    } else {
      columnResolvedFilterInfo = dimColEvaluatorInfoList.get(0);
      if (!columnResolvedFilterInfo.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        replaceCurrentNodeWithTrueFilter =
            checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
                minMaxCacheColumns, false, isStreamDataFile);
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * check if current need to be replaced with TrueFilter expression. This will happen in case
   * filter column min/max is not cached in the driver
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @Param isStreamDataFile
   * @return
   */
  private static boolean checkIfCurrentNodeToBeReplacedWithTrueFilterExpression(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    ColumnResolvedFilterInfo columnResolvedFilterInfo = null;
    if (null != filterExpressionResolverTree.getMsrColResolvedFilterInfo()) {
      columnResolvedFilterInfo = filterExpressionResolverTree.getMsrColResolvedFilterInfo();
      replaceCurrentNodeWithTrueFilter =
          checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
              minMaxCacheColumns, true, isStreamDataFile);
    } else {
      columnResolvedFilterInfo = filterExpressionResolverTree.getDimColResolvedFilterInfo();
      if (!columnResolvedFilterInfo.getDimension().hasEncoding(Encoding.IMPLICIT)) {
        replaceCurrentNodeWithTrueFilter =
            checkIfFilterColumnIsCachedInDriver(columnResolvedFilterInfo, segmentProperties,
                minMaxCacheColumns, false, isStreamDataFile);
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * Method to check whether current node needs to be replaced with true filter to avoid pruning
   * for case when filter column is not cached in the min/max cached dimension
   *
   * @param columnResolvedFilterInfo
   * @param segmentProperties
   * @param minMaxCacheColumns
   * @param isMeasure
   * @return
   */
  private static boolean checkIfFilterColumnIsCachedInDriver(
      ColumnResolvedFilterInfo columnResolvedFilterInfo, SegmentProperties segmentProperties,
      List<CarbonColumn> minMaxCacheColumns, boolean isMeasure, boolean isStreamDataFile) {
    boolean replaceCurrentNodeWithTrueFilter = false;
    CarbonColumn columnFromCurrentBlock = null;
    if (isMeasure) {
      columnFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(columnResolvedFilterInfo.getMeasure());
    } else {
      columnFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(columnResolvedFilterInfo.getDimension());
    }
    if (null != columnFromCurrentBlock) {
      // check for filter dimension in the cached column list
      if (null != minMaxCacheColumns) {
        int columnIndexInMinMaxByteArray =
            getFilterColumnIndexInCachedColumns(minMaxCacheColumns, columnFromCurrentBlock);
        if (columnIndexInMinMaxByteArray != -1) {
          columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(columnIndexInMinMaxByteArray);
        } else {
          // will be true only if column caching is enabled and current filter column is not cached
          replaceCurrentNodeWithTrueFilter = true;
        }
      } else {
        // if columns to be cached are not specified then in that case all columns will be cached
        // and  then the ordinal of column will be its index in the min/max byte array
        if (isMeasure) {
          // when read from stream data file, minmax columns cache don't include complex columns,
          // so it can not use 'segmentProperties.getLastDimensionColOrdinal()' as
          // last dimension ordinal.
          if (isStreamDataFile) {
            columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(
                segmentProperties.getDimensions().size() + columnFromCurrentBlock.getOrdinal());
          } else {
            columnResolvedFilterInfo.setColumnIndexInMinMaxByteArray(
                segmentProperties.getLastDimensionColOrdinal() + columnFromCurrentBlock
                .getOrdinal());
          }
        } else {
          columnResolvedFilterInfo
              .setColumnIndexInMinMaxByteArray(columnFromCurrentBlock.getOrdinal());
        }
      }
    }
    return replaceCurrentNodeWithTrueFilter;
  }

  /**
   * Method to check whether the filter dimension exists in the cached dimensions for a table
   *
   * @param carbonDimensionsToBeCached
   * @param filterColumn
   * @return
   */
  private static int getFilterColumnIndexInCachedColumns(
      List<CarbonColumn> carbonDimensionsToBeCached, CarbonColumn filterColumn) {
    int columnIndexInMinMaxByteArray = -1;
    int columnCounter = 0;
    for (CarbonColumn cachedColumn : carbonDimensionsToBeCached) {
      if (cachedColumn.getColumnId().equalsIgnoreCase(filterColumn.getColumnId())) {
        columnIndexInMinMaxByteArray = columnCounter;
        break;
      }
      columnCounter++;
    }
    return columnIndexInMinMaxByteArray;
  }

  /**
   * It gives filter executer based on columnar or column group
   *
   * @param dimColResolvedFilterInfo
   * @param segmentProperties
   * @return
   */
  private static FilterExecuter getExcludeFilterExecuter(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      MeasureColumnResolvedFilterInfo msrColResolvedFilterInfo,
      SegmentProperties segmentProperties) {

    if (null != msrColResolvedFilterInfo && msrColResolvedFilterInfo.getMeasure().isMeasure()) {
      CarbonMeasure measuresFromCurrentBlock =
          segmentProperties.getMeasureFromCurrentBlock(msrColResolvedFilterInfo.getMeasure());
      if (null != measuresFromCurrentBlock) {
        // update dimension and column index according to the dimension position in current block
        MeasureColumnResolvedFilterInfo msrColResolvedFilterInfoCopyObject =
            msrColResolvedFilterInfo.getCopyObject();
        msrColResolvedFilterInfoCopyObject.setMeasure(measuresFromCurrentBlock);
        msrColResolvedFilterInfoCopyObject.setColumnIndex(measuresFromCurrentBlock.getOrdinal());
        msrColResolvedFilterInfoCopyObject.setType(measuresFromCurrentBlock.getDataType());
        return new ExcludeFilterExecuterImpl(null, msrColResolvedFilterInfoCopyObject,
            segmentProperties, true);
      } else {
        return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
            msrColResolvedFilterInfo, true);
      }
    }
    CarbonDimension dimensionFromCurrentBlock =
        segmentProperties.getDimensionFromCurrentBlock(dimColResolvedFilterInfo.getDimension());
    if (null != dimensionFromCurrentBlock) {
      // update dimension and column index according to the dimension position in current block
      DimColumnResolvedFilterInfo dimColResolvedFilterInfoCopyObject =
          dimColResolvedFilterInfo.getCopyObject();
      dimColResolvedFilterInfoCopyObject.setDimension(dimensionFromCurrentBlock);
      dimColResolvedFilterInfoCopyObject.setColumnIndex(dimensionFromCurrentBlock.getOrdinal());
      return new ExcludeFilterExecuterImpl(dimColResolvedFilterInfoCopyObject, null,
          segmentProperties, false);
    } else {
      return new RestructureExcludeFilterExecutorImpl(dimColResolvedFilterInfo,
          msrColResolvedFilterInfo, false);
    }
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfExpressionContainsColumn(Expression expression) {
    if (expression instanceof ColumnExpression) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfExpressionContainsColumn(child)) {
        return true;
      }
    }

    return false;
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfLeftExpressionRequireEvaluation(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.UNKNOWN
        || !(expression instanceof ColumnExpression)) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfLeftExpressionRequireEvaluation(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method will check if a given literal expression is not a timestamp datatype
   * recursively.
   *
   * @return
   */
  public static boolean checkIfDataTypeNotTimeStamp(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.LITERAL
        && expression instanceof LiteralExpression) {
      DataType dataType = ((LiteralExpression) expression).getLiteralExpDataType();
      if (!(dataType == DataTypes.TIMESTAMP || dataType == DataTypes.DATE)) {
        return true;
      }
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfDataTypeNotTimeStamp(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method will check if a given expression contains a column expression
   * recursively.
   *
   * @return
   */
  public static boolean checkIfRightExpressionRequireEvaluation(Expression expression) {
    if (expression.getFilterExpressionType() == ExpressionType.UNKNOWN
        || !(expression instanceof LiteralExpression) && !(expression instanceof ListExpression)) {
      return true;
    }
    for (Expression child : expression.getChildren()) {
      if (checkIfRightExpressionRequireEvaluation(child)) {
        return true;
      }
    }
    return false;
  }

  /**
   * method will get the masked keys based on the keys generated from surrogates.
   *
   * @param ranges
   * @param key
   * @return byte[]
   */
  private static byte[] getMaskedKey(int[] ranges, byte[] key) {
    byte[] maskkey = new byte[ranges.length];

    for (int i = 0; i < maskkey.length; i++) {
      maskkey[i] = key[ranges[i]];
    }
    return maskkey;
  }

  /**
   * This method will return the ranges for the masked Bytes based on the key
   * Generator.
   *
   * @param queryDimensionsOrdinal
   * @param generator
   * @return
   */
  private static int[] getRangesForMaskedByte(int queryDimensionsOrdinal, KeyGenerator generator) {
    Set<Integer> integers = new TreeSet<Integer>();
    int[] range = generator.getKeyByteOffsets(queryDimensionsOrdinal);
    for (int j = range[0]; j <= range[1]; j++) {
      integers.add(j);
    }

    int[] byteIndexs = new int[integers.size()];
    int j = 0;
    for (Iterator<Integer> iterator = integers.iterator(); iterator.hasNext(); ) {
      Integer integer = iterator.next();
      byteIndexs[j++] = integer;
    }
    return byteIndexs;
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in ColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   */
  public static ColumnFilterInfo getNoDictionaryValKeyMemberForFilter(
      List<String> evaluateResultListFinal, boolean isIncludeFilter, DataType dataType)
      throws FilterUnsupportedException {
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    String result = null;
    try {
      int length = evaluateResultListFinal.size();
      String timeFormat = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
              CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
      for (int i = 0; i < length; i++) {
        result = evaluateResultListFinal.get(i);
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(result)) {
          if (dataType == DataTypes.STRING) {
            filterValuesList.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL_ARRAY);
          } else {
            filterValuesList.add(CarbonCommonConstants.EMPTY_BYTE_ARRAY);
          }
          continue;
        }
        filterValuesList.add(DataTypeUtil
            .getBytesBasedOnDataTypeForNoDictionaryColumn(result, dataType, timeFormat));
      }
    } catch (Throwable ex) {
      throw new FilterUnsupportedException("Unsupported Filter condition: " + result, ex);
    }

    java.util.Comparator<byte[]> filterNoDictValueComaparator = new java.util.Comparator<byte[]>() {

      @Override
      public int compare(byte[] filterMember1, byte[] filterMember2) {
        // TODO Auto-generated method stub
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }

    };
    Collections.sort(filterValuesList, filterNoDictValueComaparator);
    ColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setFilterListForNoDictionaryCols(filterValuesList);

    }
    return columnFilterInfo;
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in ColumnFilterInfo
   *
   * @param evaluateResultListFinal
   * @param isIncludeFilter
   * @return ColumnFilterInfo
   */
  public static ColumnFilterInfo getMeasureValKeyMemberForFilter(
      List<String> evaluateResultListFinal, boolean isIncludeFilter, DataType dataType,
      CarbonMeasure carbonMeasure) throws FilterUnsupportedException {
    List<Object> filterValuesList = new ArrayList<>(20);
    String result = null;
    try {
      int length = evaluateResultListFinal.size();
      for (int i = 0; i < length; i++) {
        result = evaluateResultListFinal.get(i);
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(result)) {
          filterValuesList.add(null);
          continue;
        }

        filterValuesList.add(DataTypeUtil
            .getMeasureValueBasedOnDataType(result, dataType, carbonMeasure.getScale(),
                carbonMeasure.getPrecision()));

      }
    } catch (Throwable ex) {
      throw new FilterUnsupportedException("Unsupported Filter condition: " + result, ex);
    }

    SerializableComparator filterMeasureComaparator =
        Comparator.getComparatorByDataTypeForMeasure(dataType);
    Collections.sort(filterValuesList, filterMeasureComaparator);
    ColumnFilterInfo columnFilterInfo = null;
    if (filterValuesList.size() > 0) {
      columnFilterInfo = new ColumnFilterInfo();
      columnFilterInfo.setIncludeFilter(isIncludeFilter);
      columnFilterInfo.setMeasuresFilterValuesList(filterValuesList);

    }
    return columnFilterInfo;
  }

  public static DataType getMeasureDataType(
      MeasureColumnResolvedFilterInfo msrColumnEvaluatorInfo) {
    if (msrColumnEvaluatorInfo.getType() == DataTypes.BOOLEAN) {
      return DataTypes.BOOLEAN;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.SHORT) {
      return DataTypes.SHORT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.INT) {
      return DataTypes.INT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.LONG) {
      return DataTypes.LONG;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.FLOAT) {
      return DataTypes.FLOAT;
    } else if (msrColumnEvaluatorInfo.getType() == DataTypes.BYTE) {
      return DataTypes.BYTE;
    } else if (DataTypes.isDecimal(msrColumnEvaluatorInfo.getType())) {
      return DataTypes.createDefaultDecimalType();
    } else {
      return DataTypes.DOUBLE;
    }
  }

  public static boolean isExcludeFilterNeedsToApply(int dictionarySize,
      int size) {
    if ((size * 100) / dictionarySize >= 60) {
      LOGGER.info("Applying CBO to convert include filter to exclude filter.");
      return true;
    }
    return false;
  }

  /**
   * Method will prepare the  dimfilterinfo instance by resolving the filter
   * expression value to its respective surrogates in the scenario of restructure.
   *
   * @param expression
   * @param defaultValues
   * @param defaultSurrogate
   * @return
   * @throws FilterUnsupportedException
   */
  public static ColumnFilterInfo getFilterListForRS(Expression expression, String defaultValues,
      int defaultSurrogate) throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    ColumnFilterInfo columnFilterInfo = null;
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    try {
      List<ExpressionResult> evaluateResultList = expression.evaluate(null).getList();
      for (ExpressionResult result : evaluateResultList) {
        if (result.getString() == null) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
          continue;
        }
        evaluateResultListFinal.add(result.getString());
      }

      for (int i = 0; i < evaluateResultListFinal.size(); i++) {
        if (evaluateResultListFinal.get(i).equals(defaultValues)) {
          filterValuesList.add(defaultSurrogate);
          break;
        }
      }
      if (filterValuesList.size() > 0) {
        columnFilterInfo = new ColumnFilterInfo();
        columnFilterInfo.setFilterList(filterValuesList);
      }
    } catch (FilterIllegalMemberException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return columnFilterInfo;
  }

  /**
   * This method will get the member based on filter expression evaluation from the
   * forward dictionary cache, this method will be basically used in restructure.
   *
   * @param expression
   * @param columnExpression
   * @param defaultValues
   * @param defaultSurrogate
   * @param isIncludeFilter
   * @return
   * @throws FilterUnsupportedException
   */
  public static ColumnFilterInfo getFilterListForAllMembersRS(Expression expression,
      ColumnExpression columnExpression, String defaultValues, int defaultSurrogate,
      boolean isIncludeFilter) throws FilterUnsupportedException {
    List<Integer> filterValuesList = new ArrayList<Integer>(20);
    List<String> evaluateResultListFinal = new ArrayList<String>(20);
    ColumnFilterInfo columnFilterInfo = null;

    try {
      RowIntf row = new RowImpl();
      if (defaultValues.equals(CarbonCommonConstants.MEMBER_DEFAULT_VAL)) {
        defaultValues = null;
      }
      row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(defaultValues,
          columnExpression.getCarbonColumn().getDataType()) });
      Boolean rslt = expression.evaluate(row).getBoolean();
      if (null != rslt && rslt == isIncludeFilter) {
        if (null == defaultValues) {
          evaluateResultListFinal.add(CarbonCommonConstants.MEMBER_DEFAULT_VAL);
        } else {
          evaluateResultListFinal.add(defaultValues);
        }
      }
    } catch (FilterIllegalMemberException e) {
      LOGGER.error(e.getMessage(), e);
    }

    if (null == defaultValues) {
      defaultValues = CarbonCommonConstants.MEMBER_DEFAULT_VAL;
    }
    columnFilterInfo = new ColumnFilterInfo();
    for (int i = 0; i < evaluateResultListFinal.size(); i++) {
      if (evaluateResultListFinal.get(i).equals(defaultValues)) {
        filterValuesList.add(defaultSurrogate);
        break;
      }
    }
    columnFilterInfo.setFilterList(filterValuesList);
    return columnFilterInfo;
  }

  private static byte[][] getFilterValuesInBytes(ColumnFilterInfo columnFilterInfo,
      boolean isExclude, KeyGenerator blockLevelKeyGenerator, int[] dimColumnsCardinality,
      int[] keys, List<byte[]> filterValuesList, int keyOrdinalOfDimensionFromCurrentBlock) {
    if (null != columnFilterInfo) {
      int[] rangesForMaskedByte =
          getRangesForMaskedByte(keyOrdinalOfDimensionFromCurrentBlock, blockLevelKeyGenerator);
      List<Integer> listOfsurrogates = null;
      if (!isExclude && columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getFilterList();
      } else if (isExclude || !columnFilterInfo.isIncludeFilter()) {
        listOfsurrogates = columnFilterInfo.getExcludeFilterList();
      }
      if (null != listOfsurrogates) {
        for (Integer surrogate : listOfsurrogates) {
          try {
            if (surrogate <= dimColumnsCardinality[keyOrdinalOfDimensionFromCurrentBlock]) {
              keys[keyOrdinalOfDimensionFromCurrentBlock] = surrogate;
              filterValuesList
                  .add(getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys)));
            } else {
              break;
            }
          } catch (KeyGenException e) {
            LOGGER.error(e.getMessage(), e);
          }
        }
      }
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);
  }

  // This function is used for calculating filter values in case when Range Column
  // is given as a Dictionary Include Column
  private static byte[][] getFilterValueInBytesForDictRange(ColumnFilterInfo columnFilterInfo,
      KeyGenerator blockLevelKeyGenerator, int[] dimColumnsCardinality, int[] keys,
      List<byte[]> filterValuesList, int keyOrdinalOfDimensionFromCurrentBlock) {
    if (null != columnFilterInfo) {
      int[] rangesForMaskedByte =
          getRangesForMaskedByte(keyOrdinalOfDimensionFromCurrentBlock, blockLevelKeyGenerator);
      List<Integer> listOfsurrogates = columnFilterInfo.getFilterList();
      if (listOfsurrogates == null || listOfsurrogates.size() > 1) {
        throw new RuntimeException(
            "Filter values cannot be null in case of range in dictionary include");
      }
      // Here we only get the first column as there can be only one range column.
      try {
        if (listOfsurrogates.get(0)
            <= dimColumnsCardinality[keyOrdinalOfDimensionFromCurrentBlock]) {
          keys[keyOrdinalOfDimensionFromCurrentBlock] = listOfsurrogates.get(0);
        } else {
          keys[keyOrdinalOfDimensionFromCurrentBlock] =
              dimColumnsCardinality[keyOrdinalOfDimensionFromCurrentBlock];
        }
        filterValuesList
            .add(getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys)));
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return filterValuesList.toArray(new byte[filterValuesList.size()][]);
  }

  /**
   * This method will be used to get the Filter key array list for blocks which do not contain
   * filter column and the column Encoding is Direct Dictionary
   *
   * @param columnFilterInfo
   * @param isExclude
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo, boolean isExclude) {
    int[] dimColumnsCardinality = new int[] { Integer.MAX_VALUE };
    int[] dimensionBitLength =
        CarbonUtil.getDimensionBitLength(dimColumnsCardinality, new int[] { 1 });
    KeyGenerator blockLevelKeyGenerator = new MultiDimKeyVarLengthGenerator(dimensionBitLength);
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = 0;
    List<byte[]> filterValuesList =
        new ArrayList<byte[]>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    return getFilterValuesInBytes(columnFilterInfo, isExclude, blockLevelKeyGenerator,
        dimColumnsCardinality, keys, filterValuesList, keyOrdinalOfDimensionFromCurrentBlock);
  }

  /**
   * Below method will be used to convert the filter surrogate keys
   * to mdkey
   *
   * @param columnFilterInfo
   * @param carbonDimension
   * @param segmentProperties
   * @param isDictRange
   * @return
   */
  public static byte[][] getKeyArray(ColumnFilterInfo columnFilterInfo,
      CarbonDimension carbonDimension, SegmentProperties segmentProperties, boolean isExclude,
      boolean isDictRange) {
    if (!carbonDimension.hasEncoding(Encoding.DICTIONARY)) {
      return columnFilterInfo.getNoDictionaryFilterValuesList()
          .toArray((new byte[columnFilterInfo.getNoDictionaryFilterValuesList().size()][]));
    }
    KeyGenerator blockLevelKeyGenerator = segmentProperties.getDimensionKeyGenerator();
    int[] dimColumnsCardinality = segmentProperties.getDimColumnsCardinality();
    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    List<byte[]> filterValuesList = new ArrayList<byte[]>(20);
    Arrays.fill(keys, 0);
    int keyOrdinalOfDimensionFromCurrentBlock = carbonDimension.getKeyOrdinal();
    if (!isDictRange) {
      return getFilterValuesInBytes(columnFilterInfo, isExclude, blockLevelKeyGenerator,
          dimColumnsCardinality, keys, filterValuesList, keyOrdinalOfDimensionFromCurrentBlock);
    } else {
      // For Dictionary Include Range Column
      return getFilterValueInBytesForDictRange(columnFilterInfo, blockLevelKeyGenerator,
          dimColumnsCardinality, keys, filterValuesList, keyOrdinalOfDimensionFromCurrentBlock);
    }
  }

  /**
   * The method is used to get the single dictionary key's mask key
   *
   * @param surrogate
   * @param carbonDimension
   * @param blockLevelKeyGenerator
   * @return
   */
  public static byte[] getMaskKey(int surrogate, CarbonDimension carbonDimension,
      KeyGenerator blockLevelKeyGenerator) {

    int[] keys = new int[blockLevelKeyGenerator.getDimCount()];
    byte[] maskedKey = null;
    Arrays.fill(keys, 0);
    int[] rangesForMaskedByte =
        getRangesForMaskedByte((carbonDimension.getKeyOrdinal()), blockLevelKeyGenerator);
    try {
      keys[carbonDimension.getKeyOrdinal()] = surrogate;
      maskedKey = getMaskedKey(rangesForMaskedByte, blockLevelKeyGenerator.generateKey(keys));
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return maskedKey;
  }

  /**
   * Method will return the start key based on KeyGenerator for the respective
   * filter resolved instance.
   *
   * @param dimensionFilter
   * @param startKey
   * @param startKeyList
   * @return long[] start key
   */
  public static void getStartKey(Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] startKey, List<long[]> startKeyList) {
    for (int i = 0; i < startKey.length; i++) {
      // The min surrogate key is 1, set it as the init value for starkey of each column level
      startKey[i] = 1;
    }
    getStartKeyWithFilter(dimensionFilter, segmentProperties, startKey, startKeyList);
  }

  /**
   * Algorithm for getting the start key for a filter
   * step 1: Iterate through each dimension and verify whether its not an exclude filter.
   * step 2: Initialize start key with the first filter member value present in each filter model
   * for the respective dimensions.
   * step 3: since its a no dictionary start key there will only actual value so compare
   * the first filter model value with respect to the dimension data type.
   * step 4: The least value will be considered as the start key of dimension by comparing all
   * its filter model.
   * step 5: create a byte array of start key which comprises of least filter member value of
   * all dimension and the indexes which will help to read the respective filter value.
   *
   * @param dimColResolvedFilterInfo
   * @param setOfStartKeyByteArray
   * @return
   */
  public static void getStartKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray) {
    Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<ColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (ColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // in case of restructure scenarios it can happen that the filter dimension is not
        // present in the current block. In those cases no need to determine the key
        CarbonDimension dimensionFromCurrentBlock = CarbonUtil
            .getDimensionFromCurrentBlock(segmentProperties.getDimensions(), entry.getKey());
        if (null == dimensionFromCurrentBlock) {
          continue;
        }
        // step 2
        byte[] noDictionaryStartKey =
            listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().get(0);
        if (setOfStartKeyByteArray.isEmpty()) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);
        } else if (null == setOfStartKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal())) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfStartKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal()),
                noDictionaryStartKey) > 0) {
          setOfStartKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryStartKey);
        }
      }
    }
  }

  /**
   * Algorithm for getting the end key for a filter
   * step 1: Iterate through each dimension and verify whether its not an exclude filter.
   * step 2: Initialize end key with the last filter member value present in each filter model
   * for the respective dimensions.(Already filter models are sorted)
   * step 3: since its a no dictionary end key there will only actual value so compare
   * the last filter model value with respect to the dimension data type.
   * step 4: The highest value will be considered as the end key of dimension by comparing all
   * its filter model.
   * step 5: create a byte array of end key which comprises of highest filter member value of
   * all dimension and the indexes which will help to read the respective filter value.
   *
   * @param dimColResolvedFilterInfo
   * @param setOfEndKeyByteArray
   * @return end key array
   */
  public static void getEndKeyForNoDictionaryDimension(
      DimColumnResolvedFilterInfo dimColResolvedFilterInfo,
      SegmentProperties segmentProperties,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray) {

    Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter =
        dimColResolvedFilterInfo.getDimensionResolvedFilterInstance();
    // step 1
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      if (!entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        List<ColumnFilterInfo> listOfDimColFilterInfo = entry.getValue();
        if (null == listOfDimColFilterInfo) {
          continue;
        }
        boolean isExcludePresent = false;
        for (ColumnFilterInfo info : listOfDimColFilterInfo) {
          if (!info.isIncludeFilter()) {
            isExcludePresent = true;
          }
        }
        if (isExcludePresent) {
          continue;
        }
        // in case of restructure scenarios it can happen that the filter dimension is not
        // present in the current block. In those cases no need to determine the key
        CarbonDimension dimensionFromCurrentBlock = CarbonUtil
            .getDimensionFromCurrentBlock(segmentProperties.getDimensions(), entry.getKey());
        if (null == dimensionFromCurrentBlock) {
          continue;
        }
        // step 2
        byte[] noDictionaryEndKey = listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList()
            .get(listOfDimColFilterInfo.get(0).getNoDictionaryFilterValuesList().size() - 1);
        if (setOfEndKeyByteArray.isEmpty()) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);
        } else if (null == setOfEndKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal())) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);

        } else if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(setOfEndKeyByteArray.get(dimensionFromCurrentBlock.getOrdinal()),
                noDictionaryEndKey) < 0) {
          setOfEndKeyByteArray.put(dimensionFromCurrentBlock.getOrdinal(), noDictionaryEndKey);
        }

      }
    }
  }

  /**
   * Method will pack all the byte[] to a single byte[] value by appending the
   * indexes of the byte[] value which needs to be read. this method will be mailny used
   * in case of no dictionary dimension processing for filters.
   *
   * @param noDictionaryValKeyList
   * @return packed key with its indexes added in starting and its actual values.
   */
  private static byte[] getKeyWithIndexesAndValues(List<byte[]> noDictionaryValKeyList) {
    ByteBuffer[] buffArr = new ByteBuffer[noDictionaryValKeyList.size()];
    int index = 0;
    for (byte[] singleColVal : noDictionaryValKeyList) {
      buffArr[index] = ByteBuffer.allocate(singleColVal.length);
      buffArr[index].put(singleColVal);
      buffArr[index++].rewind();
    }
    // byteBufer.
    return CarbonUtil.packByteBufferIntoSingleByteArray(buffArr);

  }

  /**
   * This method will fill the start key array  with the surrogate key present
   * in filterinfo instance.
   *
   * @param dimensionFilter
   * @param startKey
   */
  private static void getStartKeyWithFilter(
      Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] startKey, List<long[]> startKeyList) {
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<ColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludePresent = false;
      for (ColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludePresent = true;
        }
      }
      if (isExcludePresent) {
        continue;
      }
      // search the query dimension in current block dimensions. If the dimension is not found
      // that means the key cannot be included in start key formation.
      // Applicable for restructure scenarios
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(entry.getKey());
      if (null == dimensionFromCurrentBlock) {
        continue;
      }
      int keyOrdinalOfDimensionFromCurrentBlock = dimensionFromCurrentBlock.getKeyOrdinal();
      for (ColumnFilterInfo info : values) {
        if (keyOrdinalOfDimensionFromCurrentBlock < startKey.length) {
          if (startKey[keyOrdinalOfDimensionFromCurrentBlock] < info.getFilterList().get(0)) {
            startKey[keyOrdinalOfDimensionFromCurrentBlock] = info.getFilterList().get(0);
          }
        }
      }
      long[] newStartKey = new long[startKey.length];
      System.arraycopy(startKey, 0, newStartKey, 0, startKey.length);
      startKeyList.add(newStartKey);
    }
  }

  public static void getEndKey(Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      long[] endKey, SegmentProperties segmentProperties,
      List<long[]> endKeyList) {

    List<CarbonDimension> updatedDimListBasedOnKeyGenerator =
        getCarbonDimsMappedToKeyGenerator(segmentProperties.getDimensions());
    for (int i = 0; i < endKey.length; i++) {
      endKey[i] = getMaxValue(updatedDimListBasedOnKeyGenerator.get(i),
          segmentProperties.getDimColumnsCardinality());
    }
    getEndKeyWithFilter(dimensionFilter, segmentProperties, endKey, endKeyList);

  }

  private static List<CarbonDimension> getCarbonDimsMappedToKeyGenerator(
      List<CarbonDimension> carbonDimensions) {
    List<CarbonDimension> listOfCarbonDimPartOfKeyGen =
        new ArrayList<CarbonDimension>(carbonDimensions.size());
    for (CarbonDimension carbonDim : carbonDimensions) {
      if (CarbonUtil.hasEncoding(carbonDim.getEncoder(), Encoding.DICTIONARY) || CarbonUtil
          .hasEncoding(carbonDim.getEncoder(), Encoding.DIRECT_DICTIONARY)) {
        listOfCarbonDimPartOfKeyGen.add(carbonDim);
      }

    }
    return listOfCarbonDimPartOfKeyGen;
  }

  private static void getEndKeyWithFilter(
      Map<CarbonDimension, List<ColumnFilterInfo>> dimensionFilter,
      SegmentProperties segmentProperties, long[] endKey, List<long[]> endKeyList) {
    for (Map.Entry<CarbonDimension, List<ColumnFilterInfo>> entry : dimensionFilter.entrySet()) {
      List<ColumnFilterInfo> values = entry.getValue();
      if (null == values || !entry.getKey().hasEncoding(Encoding.DICTIONARY)) {
        continue;
      }
      boolean isExcludeFilterPresent = false;
      for (ColumnFilterInfo info : values) {
        if (!info.isIncludeFilter()) {
          isExcludeFilterPresent = true;
        }
      }
      if (isExcludeFilterPresent) {
        continue;
      }
      // search the query dimension in current block dimensions. If the dimension is not found
      // that means the key cannot be included in start key formation.
      // Applicable for restructure scenarios
      CarbonDimension dimensionFromCurrentBlock =
          segmentProperties.getDimensionFromCurrentBlock(entry.getKey());
      if (null == dimensionFromCurrentBlock) {
        continue;
      }
      int keyOrdinalOfDimensionFromCurrentBlock = dimensionFromCurrentBlock.getKeyOrdinal();
      int endFilterValue = 0;
      for (ColumnFilterInfo info : values) {
        if (keyOrdinalOfDimensionFromCurrentBlock < endKey.length) {
          endFilterValue = info.getFilterList().get(info.getFilterList().size() - 1);
          if (endFilterValue == 0) {
            endFilterValue =
                segmentProperties.getDimColumnsCardinality()[keyOrdinalOfDimensionFromCurrentBlock];
          }
          if (endKey[keyOrdinalOfDimensionFromCurrentBlock] > endFilterValue) {
            endKey[keyOrdinalOfDimensionFromCurrentBlock] = endFilterValue;
          }
        }
      }
      long[] newEndKey = new long[endKey.length];
      System.arraycopy(endKey, 0, newEndKey, 0, endKey.length);
      endKeyList.add(newEndKey);
    }

  }

  /**
   * This API will get the max value of surrogate key which will be used for
   * determining the end key of particular btree.
   *
   * @param dimCardinality
   */
  private static long getMaxValue(CarbonDimension carbonDimension, int[] dimCardinality) {
    // Get data from all the available slices of the table
    if (null != dimCardinality) {
      return dimCardinality[carbonDimension.getKeyOrdinal()];
    }
    return -1;
  }

  public static IndexKey createIndexKeyFromResolvedFilterVal(long[] startOrEndKey,
      KeyGenerator keyGenerator, byte[] startOrEndKeyForNoDictDimension) {
    IndexKey indexKey = null;
    try {
      indexKey =
          new IndexKey(keyGenerator.generateKey(startOrEndKey), startOrEndKeyForNoDictDimension);
    } catch (KeyGenException e) {
      LOGGER.error(e.getMessage(), e);
    }
    return indexKey;
  }

  /**
   * API will create an filter executer tree based on the filter resolver
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecuter getFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap, boolean isStreamDataFile) {
    return getFilterExecuterTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, null, isStreamDataFile);
  }

  /**
   * API will create an filter executer tree based on the filter resolver and minMaxColumns
   *
   * @param filterExpressionResolverTree
   * @param segmentProperties
   * @return
   */
  public static FilterExecuter getFilterExecuterTree(
      FilterResolverIntf filterExpressionResolverTree, SegmentProperties segmentProperties,
      Map<Integer, GenericQueryType> complexDimensionInfoMap,
      List<CarbonColumn> minMaxCacheColumns, boolean isStreamDataFile) {
    return createFilterExecuterTree(filterExpressionResolverTree, segmentProperties,
        complexDimensionInfoMap, minMaxCacheColumns, isStreamDataFile);
  }

  /**
   * API will prepare the Keys from the surrogates of particular filter resolver
   *
   * @param filterValues
   * @param segmentProperties
   * @param dimension
   * @param dimColumnExecuterInfo
   */
  public static void prepareKeysFromSurrogates(ColumnFilterInfo filterValues,
      SegmentProperties segmentProperties, CarbonDimension dimension,
      DimColumnExecuterFilterInfo dimColumnExecuterInfo, CarbonMeasure measures,
      MeasureColumnExecuterFilterInfo msrColumnExecuterInfo) {
    if (null != measures) {
      DataType filterColumnDataType = DataTypes.valueOf(measures.getDataType().getId());
      DataTypeConverterImpl converter = new DataTypeConverterImpl();
      Object[] keysBasedOnFilter = filterValues.getMeasuresFilterValuesList()
          .toArray((new Object[filterValues.getMeasuresFilterValuesList().size()]));
      for (int i = 0; i < keysBasedOnFilter.length; i++) {
        if (keysBasedOnFilter[i] != null) {
          keysBasedOnFilter[i] = DataTypeUtil
              .getDataBasedOnDataType(keysBasedOnFilter[i].toString(), filterColumnDataType,
                  converter);
        }
      }
      msrColumnExecuterInfo.setFilterKeys(keysBasedOnFilter, filterColumnDataType);
    } else {
      if (filterValues == null) {
        dimColumnExecuterInfo.setFilterKeys(new byte[0][]);
      } else {
        byte[][] keysBasedOnFilter =
            getKeyArray(filterValues, dimension, segmentProperties, false, false);
        if (!filterValues.isIncludeFilter() || filterValues.isOptimized()) {
          dimColumnExecuterInfo.setExcludeFilterKeys(
              getKeyArray(filterValues, dimension, segmentProperties, true, false));
        }
        dimColumnExecuterInfo.setFilterKeys(keysBasedOnFilter);
      }
    }
  }

  /**
   * method will create a default end key in case of no end key is been derived using existing
   * filter or in case of non filter queries.
   *
   * @param segmentProperties
   * @return
   * @throws KeyGenException
   */
  public static IndexKey prepareDefaultEndIndexKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    long[] dictionarySurrogateKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    int index = 0;
    int[] dimColumnsCardinality = segmentProperties.getDimColumnsCardinality();
    for (int i = 0; i < dictionarySurrogateKey.length; i++) {
      dictionarySurrogateKey[index++] = dimColumnsCardinality[i];
    }
    IndexKey endIndexKey;
    byte[] dictionaryendMdkey =
        segmentProperties.getSortColumnsGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryEndKeyBuffer = getNoDictionaryDefaultEndKey(segmentProperties);
    endIndexKey = new IndexKey(dictionaryendMdkey, noDictionaryEndKeyBuffer);
    return endIndexKey;
  }

  public static byte[] getNoDictionaryDefaultEndKey(SegmentProperties segmentProperties) {

    int numberOfNoDictionaryDimension = segmentProperties.getNumberOfNoDictSortColumns();
    // in case of non filter query when no dictionary columns are present we
    // need to set the default end key, as for non filter query
    // we need to get the last
    // block of the btree so we are setting the max byte value in the end key
    ByteBuffer noDictionaryEndKeyBuffer = ByteBuffer.allocate(
        (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE)
            + numberOfNoDictionaryDimension);
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,127,127]>
    short startPoint =
        (short) (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryEndKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryEndKeyBuffer.put((byte) 0xFF);
    }
    return noDictionaryEndKeyBuffer.array();
  }

  /**
   * method will create a default end key in case of no end key is been
   * derived using existing filter or in case of non filter queries.
   *
   * @param segmentProperties
   * @return
   * @throws KeyGenException
   */
  public static IndexKey prepareDefaultStartIndexKey(SegmentProperties segmentProperties)
      throws KeyGenException {
    IndexKey startIndexKey;
    long[] dictionarySurrogateKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    byte[] dictionaryStartMdkey =
        segmentProperties.getSortColumnsGenerator().generateKey(dictionarySurrogateKey);
    byte[] noDictionaryStartKeyArray = getNoDictionaryDefaultStartKey(segmentProperties);

    startIndexKey = new IndexKey(dictionaryStartMdkey, noDictionaryStartKeyArray);
    return startIndexKey;
  }

  public static byte[] getNoDictionaryDefaultStartKey(SegmentProperties segmentProperties) {

    int numberOfNoDictionaryDimension = segmentProperties.getNumberOfNoDictSortColumns();
    // in case of non filter query when no dictionary columns are present we
    // need to set the default start key, as for non filter query we need to get the first
    // block of the btree so we are setting the least byte value in the start key
    ByteBuffer noDictionaryStartKeyBuffer = ByteBuffer.allocate(
        (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE)
            + numberOfNoDictionaryDimension);
    // end key structure will be
    //<Offset of first No Dictionary key in 2 Bytes><Offset of second No Dictionary key in 2 Bytes>
    //<Offset of n No Dictionary key in 2 Bytes><first no dictionary column value>
    // <second no dictionary column value> <N no dictionary column value>
    //example if we have 2 no dictionary column
    //<[0,4,0,5,0,0]>
    short startPoint =
        (short) (numberOfNoDictionaryDimension * CarbonCommonConstants.SHORT_SIZE_IN_BYTE);
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryStartKeyBuffer.putShort((startPoint));
      startPoint++;
    }
    for (int i = 0; i < numberOfNoDictionaryDimension; i++) {
      noDictionaryStartKeyBuffer.put((byte) 0);
    }
    return noDictionaryStartKeyBuffer.array();
  }

  public static int compareFilterKeyBasedOnDataType(String dictionaryVal, String memberVal,
      DataType dataType) {
    try {
      if (dataType == DataTypes.BOOLEAN) {
        return Boolean.compare((Boolean.parseBoolean(dictionaryVal)),
                (Boolean.parseBoolean(memberVal)));
      } else if (dataType == DataTypes.SHORT) {
        return Short.compare((Short.parseShort(dictionaryVal)), (Short.parseShort(memberVal)));
      } else if (dataType == DataTypes.INT) {
        return Integer.compare((Integer.parseInt(dictionaryVal)), (Integer.parseInt(memberVal)));
      } else if (dataType == DataTypes.DOUBLE) {
        return Double.compare((Double.parseDouble(dictionaryVal)), (Double.parseDouble(memberVal)));
      } else if (dataType == DataTypes.LONG) {
        return Long.compare((Long.parseLong(dictionaryVal)), (Long.parseLong(memberVal)));
      } else if (dataType == DataTypes.BOOLEAN) {
        return Boolean.compare(
            (Boolean.parseBoolean(dictionaryVal)), (Boolean.parseBoolean(memberVal)));
      } else if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
        String format = CarbonUtil.getFormatFromProperty(dataType);
        SimpleDateFormat parser = new SimpleDateFormat(format);
        Date dateToStr;
        Date dictionaryDate;
        dateToStr = parser.parse(memberVal);
        dictionaryDate = parser.parse(dictionaryVal);
        return dictionaryDate.compareTo(dateToStr);
      } else if (DataTypes.isDecimal(dataType)) {
        java.math.BigDecimal javaDecValForDictVal = new java.math.BigDecimal(dictionaryVal);
        java.math.BigDecimal javaDecValForMemberVal = new java.math.BigDecimal(memberVal);
        return javaDecValForDictVal.compareTo(javaDecValForMemberVal);
      } else {
        return -1;
      }
    } catch (ParseException | NumberFormatException e) {
      return -1;
    }
  }

  /**
   * method will set the start and end key for as per the filter resolver tree
   * utilized visitor pattern inorder to populate the start and end key population.
   *
   * @param segmentProperties
   * @param filterResolver
   * @param listOfStartEndKeys
   */
  public static void traverseResolverTreeAndGetStartAndEndKey(SegmentProperties segmentProperties,
      FilterResolverIntf filterResolver, List<IndexKey> listOfStartEndKeys) {
    IndexKey searchStartKey = null;
    IndexKey searchEndKey = null;
    long[] startKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    long[] endKey = new long[segmentProperties.getNumberOfDictSortColumns()];
    List<byte[]> listOfStartKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    List<byte[]> listOfEndKeyByteArray =
        new ArrayList<byte[]>(segmentProperties.getNumberOfNoDictionaryDimension());
    SortedMap<Integer, byte[]> setOfStartKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> setOfEndKeyByteArray = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultStartValues = new TreeMap<Integer, byte[]>();
    SortedMap<Integer, byte[]> defaultEndValues = new TreeMap<Integer, byte[]>();
    List<long[]> startKeyList = new ArrayList<long[]>();
    List<long[]> endKeyList = new ArrayList<long[]>();
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolver, segmentProperties, startKey,
        setOfStartKeyByteArray, endKey, setOfEndKeyByteArray,
        startKeyList, endKeyList);
    if (endKeyList.size() > 0) {
      //get the new end key from list
      for (int i = 0; i < endKey.length; i++) {
        long[] endkeyColumnLevel = new long[endKeyList.size()];
        int j = 0;
        for (long[] oneEndKey : endKeyList) {
          //get each column level end key
          endkeyColumnLevel[j++] = oneEndKey[i];
        }
        Arrays.sort(endkeyColumnLevel);
        // get the max one as end of this column level
        endKey[i] = endkeyColumnLevel[endkeyColumnLevel.length - 1];
      }
    }

    if (startKeyList.size() > 0) {
      //get the new start key from list
      for (int i = 0; i < startKey.length; i++) {
        long[] startkeyColumnLevel = new long[startKeyList.size()];
        int j = 0;
        for (long[] oneStartKey : startKeyList) {
          //get each column level start key
          startkeyColumnLevel[j++] = oneStartKey[i];
        }
        Arrays.sort(startkeyColumnLevel);
        // get the min - 1 as start of this column level, for example if a block contains 5,6
        // the filter is 6, but that block's start key is 5, if not -1, this block will missing.
        startKey[i] = startkeyColumnLevel[0] - 1;
      }
    }

    fillDefaultStartValue(defaultStartValues, segmentProperties);
    fillDefaultEndValue(defaultEndValues, segmentProperties);
    fillNullValuesStartIndexWithDefaultKeys(setOfStartKeyByteArray, segmentProperties);
    fillNullValuesEndIndexWithDefaultKeys(setOfEndKeyByteArray, segmentProperties);
    pruneStartAndEndKeys(setOfStartKeyByteArray, listOfStartKeyByteArray);
    pruneStartAndEndKeys(setOfEndKeyByteArray, listOfEndKeyByteArray);

    if (segmentProperties.getNumberOfNoDictSortColumns() == 0) {
      listOfStartKeyByteArray = new ArrayList<byte[]>();
      listOfEndKeyByteArray = new ArrayList<byte[]>();
    } else {
      while (segmentProperties.getNumberOfNoDictSortColumns() < listOfStartKeyByteArray.size()) {
        listOfStartKeyByteArray.remove(listOfStartKeyByteArray.size() - 1);
        listOfEndKeyByteArray.remove(listOfEndKeyByteArray.size() - 1);
      }
    }

    searchStartKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(startKey, segmentProperties.getSortColumnsGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfStartKeyByteArray));

    searchEndKey = FilterUtil
        .createIndexKeyFromResolvedFilterVal(endKey, segmentProperties.getSortColumnsGenerator(),
            FilterUtil.getKeyWithIndexesAndValues(listOfEndKeyByteArray));
    listOfStartEndKeys.add(searchStartKey);
    listOfStartEndKeys.add(searchEndKey);

  }

  private static int compareFilterMembersBasedOnActualDataType(String filterMember1,
      String filterMember2, DataType dataType) {
    try {
      if (dataType == DataTypes.SHORT ||
          dataType == DataTypes.INT ||
          dataType == DataTypes.LONG ||
          dataType == DataTypes.DOUBLE) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        Double d1 = Double.parseDouble(filterMember1);
        Double d2 = Double.parseDouble(filterMember2);
        return d1.compareTo(d2);
      } else if (DataTypes.isDecimal(dataType)) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        java.math.BigDecimal val1 = new BigDecimal(filterMember1);
        java.math.BigDecimal val2 = new BigDecimal(filterMember2);
        return val1.compareTo(val2);
      } else if (dataType == DataTypes.DATE || dataType == DataTypes.TIMESTAMP) {
        if (CarbonCommonConstants.MEMBER_DEFAULT_VAL.equals(filterMember1)) {
          return 1;
        }
        String format = null;
        if (dataType == DataTypes.DATE) {
          format = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
                  CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
        } else {
          format = CarbonProperties.getInstance()
              .getProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
                  CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        }
        SimpleDateFormat parser = new SimpleDateFormat(format);
        Date date1 = null;
        Date date2 = null;
        date1 = parser.parse(filterMember1);
        date2 = parser.parse(filterMember2);
        return date1.compareTo(date2);
      } else {
        return filterMember1.compareTo(filterMember2);
      }
    } catch (ParseException | NumberFormatException e) {
      return -1;
    }
  }

  private static void fillNullValuesStartIndexWithDefaultKeys(
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      if (null == setOfStartKeyByteArray.get(dimension.getOrdinal())) {
        setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { 0 });
      }

    }
  }

  private static void fillNullValuesEndIndexWithDefaultKeys(
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      if (null == setOfStartKeyByteArray.get(dimension.getOrdinal())) {
        setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { (byte) 0xFF });
      }

    }
  }

  private static void pruneStartAndEndKeys(SortedMap<Integer, byte[]> setOfStartKeyByteArray,
      List<byte[]> listOfStartKeyByteArray) {
    for (Map.Entry<Integer, byte[]> entry : setOfStartKeyByteArray.entrySet()) {
      listOfStartKeyByteArray.add(entry.getValue());
    }
  }

  private static void fillDefaultStartValue(SortedMap<Integer, byte[]> setOfStartKeyByteArray,
      SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      setOfStartKeyByteArray.put(dimension.getOrdinal(), new byte[] { 0 });
    }

  }

  private static void fillDefaultEndValue(SortedMap<Integer, byte[]> setOfEndKeyByteArray,
      SegmentProperties segmentProperties) {
    List<CarbonDimension> allDimension = segmentProperties.getDimensions();
    for (CarbonDimension dimension : allDimension) {
      if (CarbonUtil.hasEncoding(dimension.getEncoder(), Encoding.DICTIONARY)) {
        continue;
      }
      setOfEndKeyByteArray.put(dimension.getOrdinal(), new byte[] { (byte) 0xFF });
    }
  }

  private static void traverseResolverTreeAndPopulateStartAndEndKeys(
      FilterResolverIntf filterResolverTree, SegmentProperties segmentProperties, long[] startKeys,
      SortedMap<Integer, byte[]> setOfStartKeyByteArray, long[] endKeys,
      SortedMap<Integer, byte[]> setOfEndKeyByteArray, List<long[]> startKeyList,
      List<long[]> endKeyList) {
    if (null == filterResolverTree) {
      return;
    }
    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getLeft(),
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray,
        startKeyList, endKeyList);
    filterResolverTree
        .getStartKey(segmentProperties, startKeys, setOfStartKeyByteArray, startKeyList);
    filterResolverTree.getEndKey(segmentProperties, endKeys, setOfEndKeyByteArray,
        endKeyList);

    traverseResolverTreeAndPopulateStartAndEndKeys(filterResolverTree.getRight(),
        segmentProperties, startKeys, setOfStartKeyByteArray, endKeys, setOfEndKeyByteArray,
        startKeyList, endKeyList);
  }

  /**
   * Method will find whether the expression needs to be resolved, this can happen
   * if the expression is exclude and data type is null(mainly in IS NOT NULL filter scenario)
   *
   * @param rightExp
   * @param isIncludeFilter
   * @return
   */
  public static boolean isExpressionNeedsToResolved(Expression rightExp, boolean isIncludeFilter) {
    if (!isIncludeFilter && rightExp instanceof LiteralExpression && (
        DataTypes.NULL == ((LiteralExpression) rightExp)
            .getLiteralExpDataType())) {
      return true;
    }
    for (Expression child : rightExp.getChildren()) {
      if (isExpressionNeedsToResolved(child, isIncludeFilter)) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method will print the error log.
   *
   * @param e
   */
  public static void logError(Throwable e, boolean invalidRowsPresent) {
    if (!invalidRowsPresent) {
      LOGGER.error(CarbonCommonConstants.FILTER_INVALID_MEMBER + e.getMessage(), e);
    }
  }

  /**
   * This method will compare double values for its equality and also it will preserve
   * the -0.0 and 0.0 equality as per == ,also preserve NaN equality check as per
   * java.lang.Double.equals()
   *
   * @param d1 double value for equality check
   * @param d2 double value for equality check
   * @return boolean after comparing two double values.
   */
  public static boolean nanSafeEqualsDoubles(Double d1, Double d2) {
    if ((d1.doubleValue() == d2.doubleValue()) || (Double.isNaN(d1) && Double.isNaN(d2))) {
      return true;
    }
    return false;

  }

  /**
   * This method will create default bitset group. Applicable for restructure scenarios.
   *
   * @param pageCount
   * @param totalRowCount
   * @param defaultValue
   * @return
   */
  public static BitSetGroup createBitSetGroupWithDefaultValue(int pageCount, int totalRowCount,
      boolean defaultValue) {
    BitSetGroup bitSetGroup = new BitSetGroup(pageCount);
    int numberOfRows = CarbonV3DataFormatConstants.NUMBER_OF_ROWS_PER_BLOCKLET_COLUMN_PAGE_DEFAULT;
    int pagesTobeFullFilled = totalRowCount / numberOfRows;
    int rowCountForLastPage = totalRowCount % numberOfRows;
    for (int i = 0; i < pagesTobeFullFilled; i++) {
      BitSet bitSet = new BitSet(numberOfRows);
      bitSet.set(0, numberOfRows, defaultValue);
      bitSetGroup.setBitSet(bitSet, i);
    }
    // create and fill bitset for the last page if any records are left
    if (rowCountForLastPage > 0) {
      BitSet bitSet = new BitSet(rowCountForLastPage);
      bitSet.set(0, rowCountForLastPage, defaultValue);
      bitSetGroup.setBitSet(bitSet, pagesTobeFullFilled);
    }
    return bitSetGroup;
  }

  /**
   * This method will compare the selected data against null values and
   * flip the bitSet if any null value is found
   *
   * @param dimensionColumnPage
   * @param bitSet
   */
  public static void removeNullValues(DimensionColumnPage dimensionColumnPage, BitSet bitSet,
      byte[] defaultValue) {
    if (!bitSet.isEmpty()) {
      if (null != dimensionColumnPage.getNullBits()) {
        if (!dimensionColumnPage.getNullBits().isEmpty()) {
          for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            if (dimensionColumnPage.getNullBits().get(i)) {
              bitSet.flip(i);
            }
          }
        }
      } else {
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
          if (dimensionColumnPage.compareTo(i, defaultValue) == 0) {
            bitSet.flip(i);
          }
        }
      }
    }
  }

  public static void updateIndexOfColumnExpression(Expression exp, int dimOridnalMax) {
    // if expression is null, not require to update index.
    if (exp == null) {
      return;
    }
    if (exp.getChildren() == null || exp.getChildren().size() == 0) {
      if (exp instanceof ColumnExpression) {
        ColumnExpression ce = (ColumnExpression) exp;
        CarbonColumn column = ce.getCarbonColumn();
        if (column.isDimension()) {
          ce.setColIndex(column.getOrdinal());
        } else {
          ce.setColIndex(dimOridnalMax + column.getOrdinal());
        }
      }
    } else {
      if (exp.getChildren().size() > 0) {
        List<Expression> children = exp.getChildren();
        for (int i = 0; i < children.size(); i++) {
          updateIndexOfColumnExpression(children.get(i), dimOridnalMax);
        }
      }
    }
  }

  /**
   * This method will get the no dictionary data based on filters and same
   * will be in DimColumnFilterInfo
   *
   * @param implicitColumnFilterList
   * @param isIncludeFilter
   * @return
   */
  public static ColumnFilterInfo getImplicitColumnFilterList(
      Map<String, Set<Integer>> implicitColumnFilterList, boolean isIncludeFilter) {
    ColumnFilterInfo columnFilterInfo = new ColumnFilterInfo();
    columnFilterInfo.setIncludeFilter(isIncludeFilter);
    if (null != implicitColumnFilterList) {
      columnFilterInfo.setImplicitColumnFilterBlockToBlockletsMap(implicitColumnFilterList);
    }
    return columnFilterInfo;
  }

  /**
   * This method will check for ColumnExpression with column name positionID and if found will
   * replace the InExpression with true expression. This is done to stop serialization of List
   * expression which is right children of InExpression as it can impact the query performance
   * as the size of list grows bigger.
   *
   * @param expression
   */
  public static void removeInExpressionNodeWithPositionIdColumn(Expression expression) {
    if (null != getImplicitFilterExpression(expression)) {
      setTrueExpressionAsRightChild(expression);
    }
  }

  /**
   * This method will check for ColumnExpression with column name positionID and if found will
   * replace the InExpression with true expression. This is done to stop serialization of List
   * expression which is right children of InExpression as it can impact the query performance
   * as the size of list grows bigger.
   *
   * @param expression
   */
  public static void setTrueExpressionAsRightChild(Expression expression) {
    setNewExpressionForRightChild(expression, new TrueExpression(null));
  }

  /**
   * Method to remove right child of the AND expression and set new expression for right child
   *
   * @param expression
   * @param rightChild
   */
  public static void setNewExpressionForRightChild(Expression expression, Expression rightChild) {
    // Remove the right expression node and point the expression to left node expression
    expression.findAndSetChild(((AndExpression) expression).getRight(), rightChild);
    LOGGER.info("In expression removed from the filter expression list to prevent it from"
        + " serializing on executor");
  }

  /**
   * This methdd will check if ImplictFilter is present or not
   * if it is present then return that ImplicitFilterExpression
   *
   * @param expression
   * @return
   */
  public static Expression getImplicitFilterExpression(Expression expression) {
    ExpressionType filterExpressionType = expression.getFilterExpressionType();
    if (ExpressionType.AND == filterExpressionType) {
      Expression rightExpression = ((AndExpression) expression).getRight();
      if (rightExpression instanceof InExpression) {
        List<Expression> children = rightExpression.getChildren();
        if (null != children && !children.isEmpty()) {
          Expression childExpression = children.get(0);
          // check for the positionId as the column name in ColumnExpression
          if (childExpression instanceof ColumnExpression && ((ColumnExpression) childExpression)
              .getColumnName().equalsIgnoreCase(CarbonCommonConstants.POSITION_ID)) {
            // Remove the right expression node and point the expression to left node expression
            // if 1st children is implict column positionID then 2nd children will be
            // implicit filter list
            return children.get(1);
          }
        }
      }
    }
    return null;
  }

  /**
   * This method will create implicit expression and set as right child in the current expression
   *
   * @param expression
   * @param blockIdToBlockletIdMapping
   */
  public static void createImplicitExpressionAndSetAsRightChild(Expression expression,
      Map<String, Set<Integer>> blockIdToBlockletIdMapping) {
    ColumnExpression columnExpression =
        new ColumnExpression(CarbonCommonConstants.POSITION_ID, DataTypes.STRING);
    ImplicitExpression implicitExpression = new ImplicitExpression(blockIdToBlockletIdMapping);
    InExpression inExpression = new InExpression(columnExpression, implicitExpression);
    setNewExpressionForRightChild(expression, inExpression);
    LOGGER.info("Implicit expression added to the filter expression");
  }

  /**
   * Below method will be called from include and exclude filter to convert filter values
   * based on dictionary when local dictionary is present in blocklet.
   * @param dictionary
   * Dictionary
   * @param actualFilterValues
   * actual filter values
   * @return encoded filter values
   */
  public static byte[][] getEncodedFilterValues(CarbonDictionary dictionary,
      byte[][] actualFilterValues) {
    if (null == dictionary) {
      return actualFilterValues;
    }
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX });
    int[] dummy = new int[1];
    List<byte[]> encodedFilters = new ArrayList<>();
    for (byte[] actualFilter : actualFilterValues) {
      for (int i = 1; i < dictionary.getDictionarySize(); i++) {
        if (dictionary.getDictionaryValue(i) == null) {
          continue;
        }
        if (ByteUtil.UnsafeComparer.INSTANCE
            .compareTo(actualFilter, dictionary.getDictionaryValue(i)) == 0) {
          try {
            dummy[0] = i;
            encodedFilters.add(keyGenerator.generateKey(dummy));
          } catch (KeyGenException e) {
            LOGGER.error(e.getMessage(), e);
          }
          break;
        }
      }
    }
    return getSortedEncodedFilters(encodedFilters);
  }

  /**
   * Below method will be used to sort the filter values a filter are applied using incremental
   * binary search
   * @param encodedFilters
   * encoded filter values
   * @return sorted encoded filter values
   */
  private static byte[][] getSortedEncodedFilters(List<byte[]> encodedFilters) {
    java.util.Comparator<byte[]> filterNoDictValueComaparator = new java.util.Comparator<byte[]>() {
      @Override
      public int compare(byte[] filterMember1, byte[] filterMember2) {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterMember1, filterMember2);
      }
    };
    Collections.sort(encodedFilters, filterNoDictValueComaparator);
    return encodedFilters.toArray(new byte[encodedFilters.size()][]);
  }

  /**
   * Below method will be used to get all the include filter values in case of range filters when
   * blocklet is encoded with local dictionary
   * @param expression
   * filter expression
   * @param dictionary
   * dictionary
   * @return include filter bitset
   * @throws FilterUnsupportedException
   */
  private static BitSet getIncludeDictFilterValuesForRange(Expression expression,
      CarbonDictionary dictionary) throws FilterUnsupportedException {
    ConditionalExpression conExp = (ConditionalExpression) expression;
    ColumnExpression columnExpression = conExp.getColumnList().get(0);
    BitSet includeFilterBitSet = new BitSet();
    for (int i = 2; i < dictionary.getDictionarySize(); i++) {
      if (null == dictionary.getDictionaryValue(i)) {
        continue;
      }
      try {
        RowIntf row = new RowImpl();
        String stringValue = new String(dictionary.getDictionaryValue(i),
            Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
        row.setValues(new Object[] { DataTypeUtil.getDataBasedOnDataType(stringValue,
            columnExpression.getCarbonColumn().getDataType()) });
        Boolean rslt = expression.evaluate(row).getBoolean();
        if (null != rslt) {
          if (rslt) {
            includeFilterBitSet.set(i);
          }
        }
      } catch (FilterIllegalMemberException e) {
        LOGGER.debug(e.getMessage());
      }
    }
    return includeFilterBitSet;
  }

  /**
   * Below method will used to get encoded filter values for range filter values
   * when local dictionary is present in blocklet for columns
   * If number of include filter is more than 60% of total dictionary size it will
   * convert include to exclude
   * @param includeDictValues
   * include filter values
   * @param carbonDictionary
   * dictionary
   * @param useExclude
   * to check if using exclude will be more optimized
   * @return encoded filter values
   */
  private static byte[][] getEncodedFilterValuesForRange(BitSet includeDictValues,
      CarbonDictionary carbonDictionary, boolean useExclude) {
    KeyGenerator keyGenerator = KeyGeneratorFactory
        .getKeyGenerator(new int[] { CarbonCommonConstants.LOCAL_DICTIONARY_MAX });
    List<byte[]> encodedFilterValues = new ArrayList<>();
    int[] dummy = new int[1];
    if (!useExclude) {
      try {
        for (int i = includeDictValues.nextSetBit(0);
             i >= 0; i = includeDictValues.nextSetBit(i + 1)) {
          dummy[0] = i;
          encodedFilterValues.add(keyGenerator.generateKey(dummy));
        }
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage(), e);
      }
      return encodedFilterValues.toArray(new byte[encodedFilterValues.size()][]);
    } else {
      try {
        for (int i = 1; i < carbonDictionary.getDictionarySize(); i++) {
          if (!includeDictValues.get(i) && null != carbonDictionary.getDictionaryValue(i)) {
            dummy[0] = i;
            encodedFilterValues.add(keyGenerator.generateKey(dummy));
          }
        }
      } catch (KeyGenException e) {
        LOGGER.error(e.getMessage(), e);
      }
    }
    return getSortedEncodedFilters(encodedFilterValues);
  }

  /**
   * Below method will be used to get filter executor instance for range filters
   * when local dictonary is present for in blocklet
   * @param rawColumnChunk
   * raw column chunk
   * @param exp
   * filter expression
   * @param isNaturalSorted
   * is data was already sorted
   * @return
   */
  public static FilterExecuter getFilterExecutorForRangeFilters(
      DimensionRawColumnChunk rawColumnChunk, Expression exp, boolean isNaturalSorted) {
    BitSet includeDictionaryValues;
    try {
      includeDictionaryValues =
          FilterUtil.getIncludeDictFilterValuesForRange(exp, rawColumnChunk.getLocalDictionary());
    } catch (FilterUnsupportedException e) {
      throw new RuntimeException(e);
    }
    boolean isExclude = includeDictionaryValues.cardinality() > 1 && FilterUtil
        .isExcludeFilterNeedsToApply(rawColumnChunk.getLocalDictionary().getDictionaryActualSize(),
            includeDictionaryValues.cardinality());
    byte[][] encodedFilterValues = FilterUtil
        .getEncodedFilterValuesForRange(includeDictionaryValues,
            rawColumnChunk.getLocalDictionary(), isExclude);
    FilterExecuter filterExecuter;
    if (!isExclude) {
      filterExecuter = new IncludeFilterExecuterImpl(encodedFilterValues, isNaturalSorted);
    } else {
      filterExecuter = new ExcludeFilterExecuterImpl(encodedFilterValues, isNaturalSorted);
    }
    return filterExecuter;
  }

  /**
   * This method is used to compare the filter value with min and max values.
   * This is used in case of filter queries on no dictionary column.
   *
   * @param filterValue
   * @param minMaxBytes
   * @param carbonDimension
   * @param isMin
   * @return
   */
  public static int compareValues(byte[] filterValue, byte[] minMaxBytes,
      CarbonDimension carbonDimension, boolean isMin) {
    DataType dataType = carbonDimension.getDataType();
    if (DataTypeUtil.isPrimitiveColumn(dataType) && !carbonDimension
        .hasEncoding(Encoding.DICTIONARY)) {
      Object value =
          DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(minMaxBytes, dataType);
      // filter value should be in range of max and min value i.e
      // max>filtervalue>min
      // so filter-max should be negative
      Object data = DataTypeUtil.getDataBasedOnDataTypeForNoDictionaryColumn(filterValue, dataType);
      SerializableComparator comparator = Comparator.getComparator(dataType);
      if (isMin) {
        return comparator.compare(value, data);
      } else {
        return comparator.compare(data, value);
      }
    } else {
      if (isMin) {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(minMaxBytes, filterValue);
      } else {
        return ByteUtil.UnsafeComparer.INSTANCE.compareTo(filterValue, minMaxBytes);
      }
    }
  }

  /**
   * This method is used to get default null values for a direct dictionary column
   * @param currentBlockDimension
   * @param segmentProperties
   * @return
   */
  public static byte[] getDefaultNullValue(CarbonDimension currentBlockDimension,
      SegmentProperties segmentProperties) {
    byte[] defaultValue = null;
    DirectDictionaryGenerator directDictionaryGenerator = DirectDictionaryKeyGeneratorFactory
        .getDirectDictionaryGenerator(currentBlockDimension.getDataType());
    int key = directDictionaryGenerator.generateDirectSurrogateKey(null);
    if (currentBlockDimension.isSortColumn()) {
      defaultValue = FilterUtil
          .getMaskKey(key, currentBlockDimension, segmentProperties.getSortColumnsGenerator());
    } else {
      defaultValue = FilterUtil
          .getMaskKey(key, currentBlockDimension, segmentProperties.getDimensionKeyGenerator());
    }
    return defaultValue;
  }

  public static void setMinMaxFlagForLegacyStore(boolean[] minMaxFlag,
      SegmentProperties segmentProperties) {
    int index = segmentProperties.getEachDimColumnValueSize().length + segmentProperties
        .getEachComplexDimColumnValueSize().length;
    Arrays.fill(minMaxFlag, 0, index, true);
    Arrays.fill(minMaxFlag, index, minMaxFlag.length, false);
  }

}
