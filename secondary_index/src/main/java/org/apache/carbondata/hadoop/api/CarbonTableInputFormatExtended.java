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

package org.apache.carbondata.hadoop.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil;

import org.apache.hadoop.mapreduce.JobContext;

/**
 * class specific for input output related functions of plugins
 */
public class CarbonTableInputFormatExtended {

  private static final LogService LOG =
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
  public static List<String> getFilteredSegments(JobContext job,
      CarbonTableInputFormat carbonTableInputFormat) throws IOException {
    AbsoluteTableIdentifier identifier =
        carbonTableInputFormat.getAbsoluteTableIdentifier(job.getConfiguration());
    String[] segmentsToAccess = carbonTableInputFormat.getSegmentsToAccess(job);
    Set<String> segmentsToAccessSet = new HashSet<String>();
    for (String segId : segmentsToAccess) {
      segmentsToAccessSet.add(segId);
    }
    // get all valid segments and set them into the configuration

    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(identifier);
    SegmentStatusManager.ValidAndInvalidSegmentsInfo segments =
        segmentStatusManager.getValidAndInvalidSegments();
    List<String> validSegments = segments.getValidSegments();
    //if no segments in table
    if (validSegments.size() == 0) {
      return new ArrayList<>(0);
    }
    if (segmentsToAccess.length == 0 || segmentsToAccess[0].equalsIgnoreCase("*")) {
      carbonTableInputFormat.setSegmentsToAccess(job.getConfiguration(), validSegments);
    } else {
      List<String> filteredSegmentToAccess = new ArrayList<String>();
      for (String segId : validSegments) {
        if (segmentsToAccessSet.contains(segId)) {
          filteredSegmentToAccess.add(segId);
        }
      }
      if (!filteredSegmentToAccess.containsAll(segmentsToAccessSet)) {
        List<String> filteredSegmentToAccessTemp = new ArrayList<>();
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
    CarbonTable carbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration());
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    CarbonInputFormatUtil.processFilterExpression(filter, carbonTable, null, null);
    FilterResolverIntf filterInterface =
        CarbonInputFormatUtil.resolveFilter(filter, identifier, tableProvider);
    List<String> filteredSegments = new ArrayList<>();
    // If filter is null then return all segments.
    if (filter != null) {
      List<String> setSegID = isSegmentValidAfterFilter(identifier, filterInterface,
          Arrays.asList(carbonTableInputFormat.getSegmentsToAccess(job)));
      filteredSegments.addAll(setSegID);
    } else {
      filteredSegments = Arrays.asList(carbonTableInputFormat.getSegmentsToAccess(job));
    }
    return filteredSegments;
  }

  /**
   * @return true if the filter expression lies between any one of the AbstractIndex min max values.
   */
  public static List<String> isSegmentValidAfterFilter(
      AbsoluteTableIdentifier absoluteTableIdentifier, FilterResolverIntf filterResolverIntf,
      List<String> segmentIds) throws IOException {
    TableDataMap blockletMap = DataMapStoreManager.getInstance()
        .getDataMap(absoluteTableIdentifier, BlockletDataMap.NAME,
            BlockletDataMapFactory.class.getName());
    return blockletMap.pruneSegments(segmentIds, filterResolverIntf);
  }

}