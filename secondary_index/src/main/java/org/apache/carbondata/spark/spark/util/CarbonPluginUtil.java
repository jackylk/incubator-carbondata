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
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.DeleteLoadFolders;
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
        LoadMetadataDetails[] loadMetadataDetails =
            SegmentStatusManager.readLoadMetadata(indexTable.getMetadataPath());
        // Delete marked loads
        boolean isUpdationRequired = DeleteLoadFolders
            .deleteLoadFoldersFromFileSystem(indexTable.getAbsoluteTableIdentifier(),
                isForceDeletion, loadMetadataDetails, indexTable.getMetadataPath());
        if (isUpdationRequired) {
          SegmentStatusManager.writeLoadDetailsIntoFile(
              CarbonTablePath.getTableStatusFilePath(indexTable.getTablePath()),
              loadMetadataDetails);
          LOG.info("Clean up files successful for index table: " + indexTable.getTableName());
        } else {
          LOG.info("Clean up files failed for index table: " + indexTable.getTableName());
        }
      }
    }
  }
}
