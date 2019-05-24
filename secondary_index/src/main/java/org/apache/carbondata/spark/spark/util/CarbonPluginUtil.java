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
package org.apache.carbondata.spark.spark.util;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.Segment;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.log4j.Logger;

/**
 *
 */
public final class CarbonPluginUtil {
  private static final Logger LOG =
      LogServiceFactory.getLogService(CarbonPluginUtil.class.getName());

  /**
   * This method will clean the files for all the index tables of a given table
   *
   * @param indexCarbonTables
   * @param isForceDeletion
   * @throws IOException
   */
  public static void cleanUpIndexFiles(List<CarbonTable> indexCarbonTables, boolean isForceDeletion)
      throws IOException {
    // get index table list from fact table
    for (CarbonTable indexTable : indexCarbonTables) {
      if (null != indexTable) {
        // Delete marked loads
        try {
          SegmentStatusManager.deleteLoadsAndUpdateMetadata(indexTable, isForceDeletion, null);
          LOG.info("Clean up files successful for index table: " + indexTable.getTableName());
        } catch (Exception ex) {
          LOG.error(ex);
          LOG.info("Clean up files failed for index table: " + indexTable.getTableName());
        }
      }
    }
  }

  /**
   * To delete the stale carbondata and carbonindex files from the segment
   *
   * @param indexTable
   */
  public static void deleteStaleIndexOrDataFiles(CarbonTable indexTable,
      List<Segment> validSegments) throws IOException {
    for (Segment validSegment : validSegments) {
      String segmentPath = CarbonTablePath
          .getSegmentPath(indexTable.getAbsoluteTableIdentifier().getTablePath(),
              validSegment.getSegmentNo());
      CarbonFile segmentDirPath =
          FileFactory.getCarbonFile(segmentPath, FileFactory.getFileType(segmentPath));
      CarbonFile[] allFilesOfSegment = segmentDirPath.listFiles();
      long startTimeStampFinal = validSegment.getLoadMetadataDetails().getLoadStartTime();
      long endTimeStampFinal = validSegment.getLoadMetadataDetails().getLoadEndTime();
      boolean deleteFile;
      for (CarbonFile file : allFilesOfSegment) {
        deleteFile = false;
        long fileTimestamp = getTimestamp(file);
        // check for old files before load start time and the aborted files after end time
        if ((file.getName().endsWith(CarbonTablePath.CARBON_DATA_EXT) || file.getName()
            .endsWith(CarbonTablePath.INDEX_FILE_EXT)) && (
            Long.compare(fileTimestamp, startTimeStampFinal) < 0
                || Long.compare(fileTimestamp, endTimeStampFinal) > 0)) {
          deleteFile = true;
        } else if (file.getName().endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)
            && Long.compare(fileTimestamp, startTimeStampFinal) < 0) {
          deleteFile = true;
        }
        if (deleteFile) {
          // delete the files and folders.
          try {
            LOG.info("Deleting the invalid file : " + file.getName());
            CarbonUtil.deleteFoldersAndFiles(file);
          } catch (IOException e) {
            LOG.error("Error in clean up of merged files." + e.getMessage(), e);
          } catch (InterruptedException e) {
            LOG.error("Error in clean up of merged files." + e.getMessage(), e);
          }
        }
      }
    }
  }

  public static long getTimestamp(CarbonFile eachFile) {
    String fileName = eachFile.getName();
    long timestamp = 0L;
    if (fileName.endsWith(CarbonTablePath.INDEX_FILE_EXT) || fileName
        .endsWith(CarbonTablePath.CARBON_DATA_EXT)) {
      timestamp = Long.parseLong(CarbonTablePath.DataFileUtil.getTimeStampFromFileName(fileName));
    } else if (fileName.endsWith(CarbonTablePath.MERGE_INDEX_FILE_EXT)) {
      String firstPart = fileName.substring(0, fileName.lastIndexOf('.'));
      timestamp = Long.parseLong(firstPart
          .substring(firstPart.lastIndexOf(CarbonCommonConstants.UNDERSCORE) + 1,
              firstPart.length()));
    }
    return timestamp;
  }
}
