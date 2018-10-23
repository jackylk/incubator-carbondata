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
package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.DataMapChooser;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.DataMapUtil;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.datamap.dev.expr.DataMapExprWrapper;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.statusmanager.SegmentUpdateStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.log4j.Logger;
import org.apache.spark.util.CarbonInternalScalaUtil;

/**
 * class specific for input output related functions of plugins
 */
public class CarbonTableInputFormatExtended {

  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonTableInputFormatExtended.class.getName());

  /**
   * Used to get the valid segments after applying the following conditions.
   * 1. if user has specified segments for the parent table then those segments would be considered
   * and valid segments would be filtered.
   * 2. if user has not specified segments then all valid segments would be considered for scanning.
   *
   * @param job
   * @return
   * @throws IOException
   */
  public static List<Segment> getFilteredSegments(JobContext job,
      CarbonTableInputFormat carbonTableInputFormat) throws IOException {
    CarbonTable carbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration());
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    // copy dynamic set segment property from parent table to child index table
    setQuerySegmentForIndexTable(job.getConfiguration(), carbonTable);
    AbsoluteTableIdentifier identifier = carbonTable.getAbsoluteTableIdentifier();
    ReadCommittedScope readCommittedScope =
        carbonTableInputFormat.getReadCommitted(job, identifier);
    Segment[] segmentsToAccess =
        carbonTableInputFormat.getSegmentsToAccess(job, readCommittedScope);
    Set<Segment> segmentsToAccessSet = new HashSet<Segment>();
    for (Segment segId : segmentsToAccess) {
      segmentsToAccessSet.add(segId);
    }
    // get all valid segments and set them into the configuration

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo segments =
        segmentStatusManager.getValidAndInvalidSegments();
    List<Segment> validSegments = segments.getValidSegments();
    //if no segments in table
    if (validSegments.size() == 0) {
      return new ArrayList<>(0);
    }
    if (segmentsToAccess.length == 0 || segmentsToAccess[0].getSegmentNo().equalsIgnoreCase("*")) {
      carbonTableInputFormat.setSegmentsToAccess(job.getConfiguration(), validSegments);
    } else {
      List<Segment> filteredSegmentToAccess = new ArrayList<Segment>();
      for (Segment segment : validSegments) {
        if (segmentsToAccessSet.contains(segment)) {
          filteredSegmentToAccess.add(segment);
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentsToAccessSet)) {
        List<Segment> filteredSegmentToAccessTemp = new ArrayList<>();
        filteredSegmentToAccessTemp.addAll(filteredSegmentToAccess);
        filteredSegmentToAccessTemp.removeAll(segmentsToAccessSet);
        LOG.info(
            "Segments ignored are : " + Arrays.toString(filteredSegmentToAccessTemp.toArray()));
      }

      //if no valid segments after filteration
      if (filteredSegmentToAccess.size() == 0) {
        return new ArrayList<>(0);
      } else {
        carbonTableInputFormat.setSegmentsToAccess(job.getConfiguration(), filteredSegmentToAccess);
      }
    }
    //    return getSplitsInternal(job, true);
    // process and resolve the expression
    Expression filter = carbonTableInputFormat.getFilterPredicates(job.getConfiguration());
    carbonTable.processFilterExpression(filter, null, null);
    FilterResolverIntf filterInterface = carbonTable.resolveFilter(filter, identifier);
    List<Segment> filteredSegments = new ArrayList<>();
    // If filter is null then return all segments.
    List<Segment> segmentIds =
        Arrays.asList(carbonTableInputFormat.getSegmentsToAccess(job, readCommittedScope));
    if (filter != null) {
      // refresh the segments if needed
      LoadMetadataDetails[] loadMetadataDetails = readCommittedScope.getSegmentList();
      SegmentUpdateStatusManager updateStatusManager =
          new SegmentUpdateStatusManager(carbonTable, loadMetadataDetails);
      carbonTableInputFormat
          .refreshSegmentCacheIfRequired(job, carbonTable, updateStatusManager, segmentIds);
      List<Segment> setSegID =
          isSegmentValidAfterFilter(job.getConfiguration(), carbonTable, filterInterface,
              segmentIds);
      filteredSegments.addAll(setSegID);
    } else {
      filteredSegments = segmentIds;
    }
    return filteredSegments;
  }

  /**
   * @return true if the filter expression lies between any one of the AbstractIndex min max values.
   */
  public static List<Segment> isSegmentValidAfterFilter(Configuration configuration,
      CarbonTable carbonTable, FilterResolverIntf filterResolverIntf, List<Segment> segmentIds)
      throws IOException {
    TableDataMap blockletMap = DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable);
    DataMapExprWrapper dataMapExprWrapper =
        DataMapChooser.getDefaultDataMap(carbonTable, filterResolverIntf);
    DataMapUtil.loadDataMaps(carbonTable, dataMapExprWrapper, segmentIds,
        CarbonTableInputFormat.getPartitionsToPrune(configuration));
    return blockletMap.pruneSegments(segmentIds, filterResolverIntf);
  }

  /**
   * To copy dynamic set segment property form parent table to index table
   */
  private static void setQuerySegmentForIndexTable(Configuration conf, CarbonTable carbonTable) {
    if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
      String dbName = carbonTable.getDatabaseName();
      String tbName = CarbonInternalScalaUtil.getParentTableName(carbonTable);
      String segmentNumbersFromProperty = CarbonProperties.getInstance()
          .getProperty(CarbonCommonConstants.CARBON_INPUT_SEGMENTS + dbName + "." + tbName, "*");
      if (!segmentNumbersFromProperty.trim().equals("*")) {
        CarbonTableInputFormat.setSegmentsToAccess(conf,
            Segment.toSegmentList(segmentNumbersFromProperty.split(","), null));
      }
    }
  }

}