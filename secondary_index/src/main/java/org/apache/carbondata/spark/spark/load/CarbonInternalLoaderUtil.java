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

package org.apache.carbondata.spark.spark.load;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.mutate.CarbonUpdateUtil;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.util.CarbonLoaderUtil;

import org.apache.spark.util.CarbonInternalScalaUtil;

/**
 *
 */
public class CarbonInternalLoaderUtil {

  private static final LogService LOGGER =
      LogServiceFactory.getLogService(CarbonInternalLoaderUtil.class.getName());

  public static List<String> getListOfValidSlices(LoadMetadataDetails[] details) {
    List<String> activeSlices =
        new ArrayList<String>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
    for (LoadMetadataDetails oneLoad : details) {
      if (SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.MARKED_FOR_UPDATE.equals(oneLoad.getSegmentStatus())) {
        activeSlices.add(oneLoad.getLoadName());
      }
    }
    return activeSlices;
  }

  /**
   * This method will return the mapping of valid segments to segment laod start time
   *
   * @param details
   * @return
   */
  public static Map<String, Long> getSegmentToLoadStartTimeMapping(LoadMetadataDetails[] details) {
    Map<String, Long> segmentToLoadStartTimeMap = new HashMap<>(details.length);
    for (LoadMetadataDetails oneLoad : details) {
      // valid segments will only have Success status
      if (SegmentStatus.SUCCESS.equals(oneLoad.getSegmentStatus())
          || SegmentStatus.LOAD_PARTIAL_SUCCESS.equals(oneLoad.getSegmentStatus())) {
        segmentToLoadStartTimeMap.put(oneLoad.getLoadName(), oneLoad.getLoadStartTime());
      }
    }
    return segmentToLoadStartTimeMap;
  }

  /**
   * Append default File system scheme if not added to the filePath
   * This required for
   *
   * @param filePath
   */
  public static String checkAndAppendFileSystemURIScheme(String filePath) {
    String currentPath = filePath;
    if (checkIfPrefixExists(filePath)) {
      return currentPath;
    }
    if (!filePath.startsWith("/")) {
      filePath = "/" + filePath;
    }
    currentPath = filePath;
    String defaultFsUrl = FileFactory.getConfiguration().get(CarbonCommonConstants.FS_DEFAULT_FS);
    if (defaultFsUrl == null) {
      return currentPath;
    }
    return defaultFsUrl + currentPath;
  }

  private static boolean checkIfPrefixExists(String path) {
    if (null == path) {
      return false;
    }
    final String lowerPath = path.toLowerCase(Locale.getDefault());
    return lowerPath.startsWith(CarbonCommonConstants.HDFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.VIEWFSURL_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.LOCAL_FILE_PREFIX) || lowerPath
        .startsWith(CarbonCommonConstants.ALLUXIOURL_PREFIX);
  }

  /**
   * This API will write the load level metadata for the loadmanagement module inorder to
   * manage the load and query execution management smoothly.
   *
   * @param loadMetadataDetails
   * @param validSegments
   * @param databaseName
   * @param tableName
   * @param carbonTable
   * @return boolean which determines whether status update is done or not.
   * @throws IOException
   */
  public static boolean recordLoadMetadata(List<LoadMetadataDetails> loadMetadataDetails,
      List<String> validSegments, String databaseName, String tableName,
      CarbonTable carbonTable) throws IOException {
    boolean status = false;
    String metaDataFilepath = carbonTable.getMetadataPath();
    AbsoluteTableIdentifier absoluteTableIdentifier = carbonTable.getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for table" + databaseName + "." + tableName
            + " for table status updation");

        LoadMetadataDetails[] listOfLoadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(metaDataFilepath);

        List<LoadMetadataDetails> listOfLoadFolderDetailsForFact =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);

        if (null != listOfLoadFolderDetailsArray) {
          for (LoadMetadataDetails loadMetadata : listOfLoadFolderDetailsArray) {
            listOfLoadFolderDetailsForFact.add(loadMetadata);
          }
        }
        listOfLoadFolderDetailsForFact.addAll(loadMetadataDetails);

        List<String> indexTables = CarbonInternalScalaUtil.getIndexesTables(carbonTable);
        if (!indexTables.isEmpty()) {
          List<LoadMetadataDetails> newSegmentDetailsListForIndexTable =
              new ArrayList<>(validSegments.size());
          for (String segmentId : validSegments) {
            LoadMetadataDetails newSegmentDetailsObject = new LoadMetadataDetails();
            newSegmentDetailsObject.setLoadName(segmentId);
            newSegmentDetailsListForIndexTable.add(newSegmentDetailsObject);
          }
          for (String indexTableName : indexTables) {
            CarbonTable indexTable = CarbonMetadata.getInstance().getCarbonTable(
                absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName()
                    + CarbonCommonConstants.UNDERSCORE + indexTableName);
            List<LoadMetadataDetails> indexTableDetailsList = CarbonInternalScalaUtil
                .getTableStatusDetailsForIndexTable(listOfLoadFolderDetailsForFact, indexTable,
                    newSegmentDetailsListForIndexTable);

            segmentStatusManager.writeLoadDetailsIntoFile(
                CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath()),
                indexTableDetailsList
                    .toArray(new LoadMetadataDetails[indexTableDetailsList.size()]));
          }
        } else if (CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
          segmentStatusManager.writeLoadDetailsIntoFile(
              metaDataFilepath + CarbonCommonConstants.FILE_SEPARATOR
                  + CarbonTablePath.TABLE_STATUS_FILE, listOfLoadFolderDetailsForFact
                  .toArray(new LoadMetadataDetails[listOfLoadFolderDetailsForFact.size()]));
        }
        status = true;
      } else {
        LOGGER.error(
            "Not able to acquire the lock for Table status updation for table " + databaseName + "."
                + tableName);
      }
    } catch (IOException e) {
      status = false;
      LOGGER.error(
          "Not able to acquire the lock for Table status updation for table " + databaseName + "."
              + tableName);
    }
    finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + databaseName + "."
            + tableName);
      } else {
        LOGGER.error("Unable to unlock Table lock for table" + databaseName + "." + tableName
            + " during table status updation");
      }
    }
    return status;
  }

  /**
   * method to update table status in case of IUD Update Delta Compaction.
   *
   * @param indexCarbonTable
   * @param loadsToMerge
   * @param mergedLoadNumber
   * @param carbonLoadModel
   * @return
   */
  public static boolean updateLoadMetadataWithMergeStatus(CarbonTable indexCarbonTable,
      String[] loadsToMerge, String mergedLoadNumber, CarbonLoadModel carbonLoadModel,
      long mergeLoadStartTime) throws IOException {
    boolean tableStatusUpdationStatus = false;
    List<String> loadMergeList = new ArrayList<>(Arrays.asList(loadsToMerge));
    AbsoluteTableIdentifier absoluteTableIdentifier =
        carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable().getAbsoluteTableIdentifier();
    SegmentStatusManager segmentStatusManager = new SegmentStatusManager(absoluteTableIdentifier);

    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();

    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for the table " + carbonLoadModel.getDatabaseName() + "."
            + carbonLoadModel.getTableName() + " for table status updation ");
        LoadMetadataDetails[] loadDetails =
            SegmentStatusManager.readLoadMetadata(indexCarbonTable.getMetadataPath());

        long modificationOrDeletionTimeStamp = CarbonUpdateUtil.readCurrentTime();
        for (LoadMetadataDetails loadDetail : loadDetails) {
          // check if this segment is merged.
          if (loadMergeList.contains(loadDetail.getLoadName()) || loadMergeList
              .contains(loadDetail.getMergedLoadName())) {
            // if the compacted load is deleted after the start of the compaction process,
            // then need to discard the compaction process and treat it as failed compaction.
            if (loadDetail.getSegmentStatus() == SegmentStatus.MARKED_FOR_DELETE) {
              LOGGER.error("Compaction is aborted as the segment " + loadDetail.getLoadName()
                  + " is deleted after the compaction is started.");
              return false;
            }
            loadDetail.setSegmentStatus(SegmentStatus.COMPACTED);
            loadDetail.setModificationOrdeletionTimesStamp(modificationOrDeletionTimeStamp);
            loadDetail.setMergedLoadName(mergedLoadNumber);
          }
        }

        // create entry for merged one.
        LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
        loadMetadataDetails.setPartitionCount("0");
        loadMetadataDetails.setSegmentStatus(SegmentStatus.SUCCESS);
        long loadEnddate = CarbonUpdateUtil.readCurrentTime();
        loadMetadataDetails.setLoadEndTime(loadEnddate);
        CarbonTable carbonTable = carbonLoadModel.getCarbonDataLoadSchema().getCarbonTable();
        loadMetadataDetails.setLoadName(mergedLoadNumber);
        CarbonLoaderUtil
            .addDataIndexSizeIntoMetaEntry(loadMetadataDetails, mergedLoadNumber, carbonTable);
        loadMetadataDetails.setLoadStartTime(mergeLoadStartTime);
        loadMetadataDetails.setPartitionCount("0");
        // if this is a major compaction then set the segment as major compaction.
        List<LoadMetadataDetails> updatedDetailsList = new ArrayList<>(Arrays.asList(loadDetails));

        // put the merged folder entry
        updatedDetailsList.add(loadMetadataDetails);
        segmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(indexCarbonTable.getTablePath()),
            updatedDetailsList.toArray(new LoadMetadataDetails[updatedDetailsList.size()]));
        tableStatusUpdationStatus = true;
      } else {
        LOGGER.error(
            "Could not able to obtain lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + "for table status updation");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info("Table unlocked successfully after table status updation" + carbonLoadModel
            .getDatabaseName() + "." + carbonLoadModel.getTableName());
      } else {
        LOGGER.error(
            "Unable to unlock Table lock for table" + carbonLoadModel.getDatabaseName() + "."
                + carbonLoadModel.getTableName() + " during table status updation");
      }
    }
    return tableStatusUpdationStatus;
  }

}
