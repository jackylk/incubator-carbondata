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

package org.apache.carbondata.core.metadata.schema.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.apache.carbondata.api.SegmentManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.metadata.SegmentFileStore;
import org.apache.carbondata.core.statusmanager.FileFormat;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatus;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

class SegmentManagerImpl implements SegmentManager {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(SegmentManagerImpl.class.getName());

  private CarbonTable carbonTable;

  SegmentManagerImpl(CarbonTable carbonTable) {
    this.carbonTable = carbonTable;
  }

  /**
   * Create and return new Segment and record a new entry in table status file
   */
  @Override
  public String createSegment(FileFormat format)
      throws IOException {
    int segmentId;
    if (FileFactory.isFileExist(carbonTable.getTablePath())) {
      SegmentStatusManager segmentStatusManager =
          new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier());
      ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
      try {
        if (carbonLock.lockWithRetries()) {
          LOGGER.info("Acquired lock for table" + carbonTable.getDatabaseName() + "." + carbonTable
              .getTableName() + " for table get or create online segment");
          LoadMetadataDetails[] details =
              SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
          List<LoadMetadataDetails> listOfLoadFolderDetails =
              new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
          Collections.addAll(listOfLoadFolderDetails, details);

          segmentId = SegmentStatusManager.createNewSegmentId(details);
          LoadMetadataDetails loadMetadataDetails = new LoadMetadataDetails();
          loadMetadataDetails.setFileFormat(format);
          loadMetadataDetails.setLoadName(String.valueOf(segmentId));
          loadMetadataDetails.setLoadStartTime(System.currentTimeMillis());
          loadMetadataDetails.setSegmentStatus(SegmentStatus.INSERT_IN_PROGRESS);
          listOfLoadFolderDetails.add(loadMetadataDetails);

          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()),
              listOfLoadFolderDetails.toArray(new LoadMetadataDetails[0]));

        } else {
          LOGGER.error("Not able to acquire the lock for table get or create segment for table "
              + carbonTable.getDatabaseName() + "." + carbonTable.getTableName());
          throw new IOException("Failed to create segment");
        }
      } finally {
        if (carbonLock.unlock()) {
          LOGGER.info("Table unlocked successfully after table get or create segment" + carbonTable
              .getDatabaseName() + "." + carbonTable.getDatabaseName());
        } else {
          LOGGER.error("Unable to unlock table lock for table" + carbonTable.getDatabaseName() + "."
              + carbonTable.getTableName() + " during table get or create segment");
        }
      }
    } else {
      throw new IOException("Table does not exists: " + carbonTable.getTablePath());
    }
    return String.valueOf(segmentId);
  }

  /**
   * If the passing online segment is full, trigger 'handoff' operation,
   * create and return a new online segment path.
   * Otherwise return the passing path
   */
  @Override
  public String getOrCreateSegment(String segmentPath, FileFormat format) throws IOException {
    // check if segmentPath exists
    if (FileFactory.isFileExist(segmentPath, FileFactory.getFileType(segmentPath))) {
      CarbonFile segmentFile =
          FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
      long size = 0;
      if (segmentFile.isDirectory()) {
        CarbonFile[] carbonFiles = segmentFile.listFiles();
        for (CarbonFile carbonFile : carbonFiles) {
          size += carbonFile.getSize();
        }
      }
      if (size <= CarbonCommonConstants.SEGMENT_SIZE_DEFAULT) {
        return segmentPath;
      } else {
        commitSegment(CarbonTablePath.getSegmentId(segmentPath));
        String newSegmentId = createSegment(format);
        segmentPath = carbonTable.getSegmentPath(newSegmentId);
      }
    }
    return segmentPath;
  }

  /**
   * create segment in table meta path and record segment status as Success
   */
  @Override
  public boolean commitSegment(String segmentId) throws IOException {
    org.apache.carbondata.core.datamap.Segment
        segment = new org.apache.carbondata.core.datamap.Segment(segmentId,
        SegmentFileStore.genSegmentFileName(segmentId, String.valueOf(System.nanoTime()))
            + CarbonTablePath.SEGMENT_EXT, carbonTable.getSegmentPath(segmentId),
        new HashMap<String, String>());
    boolean isSuccess = SegmentFileStore.writeSegmentFile(carbonTable, segment);

    if (isSuccess) {
      return SegmentFileStore.updateSegmentFile(
          carbonTable,
          segmentId,
          segment.getSegmentFileName(),
          carbonTable.getTableId(),
          new SegmentFileStore(carbonTable.getTablePath(), segment.getSegmentFileName()),
          SegmentStatus.SUCCESS);
    } else {
      throw new IOException("Commit segment with Id " + segmentId + " failed.");
    }
  }

  /**
   * delete specified segment from the table
   */
  @Override
  public void deleteSegment(String segmentId) throws IOException {
    SegmentStatusManager segmentStatusManager =
        new SegmentStatusManager(carbonTable.getAbsoluteTableIdentifier());
    ICarbonLock carbonLock = segmentStatusManager.getTableStatusLock();
    try {
      if (carbonLock.lockWithRetries()) {
        LOGGER.info("Acquired lock for table" + carbonTable.getDatabaseName() + "." + carbonTable
            .getTableName() + " for table delete segment");
        LoadMetadataDetails[] details =
            SegmentStatusManager.readLoadMetadata(carbonTable.getMetadataPath());
        List<LoadMetadataDetails> listOfLoadFolderDetails =
            new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
        Collections.addAll(listOfLoadFolderDetails, details);

        for (LoadMetadataDetails loadMetadataDetail : listOfLoadFolderDetails) {
          if (loadMetadataDetail.getLoadName().equalsIgnoreCase(segmentId)) {
            loadMetadataDetail.setSegmentStatus(SegmentStatus.MARKED_FOR_DELETE);
          }
        }
        SegmentStatusManager.writeLoadDetailsIntoFile(
            CarbonTablePath.getTableStatusFilePath(carbonTable.getTablePath()),
            listOfLoadFolderDetails.toArray(new LoadMetadataDetails[0]));

      } else {
        LOGGER.error(
            "Not able to acquire the lock for table delete segment for table " + carbonTable
                .getDatabaseName() + "." + carbonTable.getTableName());
        throw new IOException("Failed to delete segment");
      }
    } finally {
      if (carbonLock.unlock()) {
        LOGGER.info(
            "Table unlocked successfully after table delete segment" + carbonTable.getDatabaseName()
                + "." + carbonTable.getDatabaseName());
      } else {
        LOGGER.error("Unable to unlock table lock for table" + carbonTable.getDatabaseName() + "."
            + carbonTable.getTableName() + " during table delete segment");
      }
    }
  }

}
