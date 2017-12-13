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
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.CarbonMetadata;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.statusmanager.LoadMetadataDetails;
import org.apache.carbondata.core.statusmanager.SegmentStatusManager;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonProperty;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonStorePath;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.carbondata.processing.util.DeleteLoadFolders;
import org.apache.carbondata.spark.core.CarbonInternalCommonConstants;

import org.apache.spark.util.CarbonInternalScalaUtil;

/**
 *
 */
public final class CarbonPluginUtil {
  private static final LogService LOG =
      LogServiceFactory.getLogService(SegmentStatusManager.class.getName());

  /**
   * This method will update the deletion status for all the index tables
   *
   * @param absoluteTableIdentifier
   * @param loadIds
   * @throws IOException
   */
  public static void updateTableStatusForIndexTables(
      AbsoluteTableIdentifier absoluteTableIdentifier, List<String> loadIds,
      CarbonTable carbonTable) throws IOException {
    // get the list of index tables from carbon table
    List<String> indexTableList = CarbonInternalScalaUtil.getIndexesTables(carbonTable);
    for (String indexTableName : indexTableList) {
      CarbonTable indexTable = CarbonMetadata.getInstance().getCarbonTable(
          absoluteTableIdentifier.getCarbonTableIdentifier().getDatabaseName()
              + CarbonCommonConstants.UNDERSCORE + indexTableName);
      if (null != indexTable) {
        CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(indexTable.getAbsoluteTableIdentifier().getTablePath(),
                indexTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier());
        String tableStatusFilePath = carbonTablePath.getTableStatusFilePath();
        if (!CarbonUtil.isFileExists(tableStatusFilePath)) {
          LOG.info("Table status file does not exist for index table: " + indexTableName);
          continue;
        }
        LoadMetadataDetails[] loadFolderDetailsArray =
            SegmentStatusManager.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
        if (null != loadFolderDetailsArray && loadFolderDetailsArray.length > 0) {
          List<String> invalidLoads =
              new ArrayList<>(CarbonCommonConstants.DEFAULT_COLLECTION_SIZE);
          try {
            SegmentStatusManager
                .updateDeletionStatus(carbonTable.getAbsoluteTableIdentifier(), loadIds,
                    carbonTable.getMetaDataFilepath());
            if (invalidLoads.size() > 0) {
              LOG.audit("Delete segment by Id is successfull for $dbName.$tableName.");
            } else {
              LOG.error(
                  "Delete segment by Id is failed. Invalid ID is: " + invalidLoads.toString());
            }
          } catch (Exception ex) {
            LOG.error(ex.getMessage());
          }
          if (!invalidLoads.isEmpty()) {
            // handle the case for inconsistency scenario
            LOG.info("There is some inconsistency in table status file of index table "
                + indexTableName);
          } else {
            // update table status file
            SegmentStatusManager.writeLoadDetailsIntoFile(carbonTablePath.getTableStatusFilePath(),
                loadFolderDetailsArray);
          }
        }
      }
    }
  }

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
        CarbonTablePath carbonTablePath = CarbonStorePath
            .getCarbonTablePath(indexTable.getAbsoluteTableIdentifier().getTablePath(),
                indexTable.getAbsoluteTableIdentifier().getCarbonTableIdentifier());
        LoadMetadataDetails[] loadMetadataDetails =
            SegmentStatusManager.readLoadMetadata(carbonTablePath.getMetadataDirectoryPath());
        // Delete marked loads
        boolean isUpdationRequired = DeleteLoadFolders
            .deleteLoadFoldersFromFileSystem(indexTable.getAbsoluteTableIdentifier(),
                isForceDeletion,
                loadMetadataDetails);
        if (isUpdationRequired) {
          SegmentStatusManager.writeLoadDetailsIntoFile(carbonTablePath.getTableStatusFilePath(),
              loadMetadataDetails);
          LOG.info("Clean up files successful for index table: " + indexTableName);
        } else {
          LOG.info("Clean up files failed for index table: " + indexTableName);
        }
      }
    }
  }

  /**
   *
   *
   */
  public static void registerIntenalProperty() throws IllegalAccessException {
    Field[] declaredFields = CarbonInternalCommonConstants.class.getDeclaredFields();
    Set<String> propertySet =  new HashSet<String>();
    for (Field field : declaredFields) {
      if (field.isAnnotationPresent(CarbonProperty.class)) {
        propertySet.add(field.get(field.getName()).toString());
      }
    }
    CarbonProperties.getInstance().addPropertyToPropertySet(propertySet);
  }
}
