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
