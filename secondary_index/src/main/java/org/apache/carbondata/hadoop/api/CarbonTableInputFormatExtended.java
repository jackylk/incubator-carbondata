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
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datamap.TableDataMap;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.readcommitter.ReadCommittedScope;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.filter.SingleTableProvider;
import org.apache.carbondata.core.scan.filter.TableProvider;
import org.apache.carbondata.core.scan.filter.resolver.FilterResolverIntf;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;

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
  public static List<Segment> getFilteredSegments(JobContext job,
      CarbonTableInputFormat carbonTableInputFormat) throws IOException {
    AbsoluteTableIdentifier identifier =
        carbonTableInputFormat.getAbsoluteTableIdentifier(job.getConfiguration());
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
    CarbonTable carbonTable = carbonTableInputFormat.getOrCreateCarbonTable(job.getConfiguration());
    TableProvider tableProvider = new SingleTableProvider(carbonTable);
    // this will be null in case of corrupt schema file.
    if (null == carbonTable) {
      throw new IOException("Missing/Corrupt schema file for table.");
    }
    carbonTable.processFilterExpression(filter, null, null);
    FilterResolverIntf filterInterface =
        carbonTable.resolveFilter(filter, tableProvider);
    List<Segment> filteredSegments = new ArrayList<>();
    // If filter is null then return all segments.
    if (filter != null) {
      List<Segment> setSegID = isSegmentValidAfterFilter(carbonTable, filterInterface,
          Arrays.asList(carbonTableInputFormat.getSegmentsToAccess(job, readCommittedScope)));
      filteredSegments.addAll(setSegID);
    } else {
      filteredSegments =
          Arrays.asList(carbonTableInputFormat.getSegmentsToAccess(job, readCommittedScope));
    }
    return filteredSegments;
  }

  /**
   * @return true if the filter expression lies between any one of the AbstractIndex min max values.
   */
  public static List<Segment> isSegmentValidAfterFilter(
      CarbonTable carbonTable, FilterResolverIntf filterResolverIntf,
      List<Segment> segmentIds) throws IOException {
    TableDataMap blockletMap = DataMapStoreManager.getInstance().getDefaultDataMap(carbonTable);
    return blockletMap.pruneSegments(segmentIds, filterResolverIntf);
  }

}