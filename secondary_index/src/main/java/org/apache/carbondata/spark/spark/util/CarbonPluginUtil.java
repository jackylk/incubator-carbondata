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

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.DeleteLoadFolders;
import org.apache.carbondata.core.util.path.CarbonTablePath;

import org.apache.spark.util.CarbonInternalScalaUtil;

/**
 *
 */
public final class CarbonPluginUtil {
  private static final LogService LOG =
      LogServiceFactory.getLogService(CarbonPluginUtil.class.getName());

  /**
   * This method will clean the files for all the index tables of a given table
   *
   * @param factTable
   * @param carbonStoreLocation
   * @param isForceDeletion
   * @throws IOException
   */
  public static void cleanUpIndexFiles(CarbonTable factTable, String carbonStoreLocation,
      boolean isForceDeletion) throws IOException {
    // get index table list from fact table
    List<String> indexTables = CarbonInternalScalaUtil.getIndexesTables(factTable);
    for (String indexTableName : indexTables) {
      CarbonTable indexTable = CarbonMetadata.getInstance().getCarbonTable(
          factTable.getCarbonTableIdentifier().getDatabaseName() + CarbonCommonConstants.UNDERSCORE
              + indexTableName);
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
          LOG.info("Clean up files successful for index table: " + indexTableName);
        } else {
          LOG.info("Clean up files failed for index table: " + indexTableName);
        }
      }
    }
  }
}
