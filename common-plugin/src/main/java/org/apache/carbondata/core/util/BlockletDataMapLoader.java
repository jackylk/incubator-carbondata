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

package org.apache.carbondata.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datamap.dev.DataMap;
import org.apache.carbondata.core.indexstore.BlockMetaInfo;
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifier;
import org.apache.carbondata.core.indexstore.TableBlockIndexUniqueIdentifierWrapper;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMap;
import org.apache.carbondata.core.indexstore.blockletindex.BlockletDataMapModel;
import org.apache.carbondata.core.indexstore.blockletindex.SegmentIndexFileStore;
import org.apache.carbondata.core.memory.MemoryException;

public class BlockletDataMapLoader {

  /**
   * This method loads the dataMap for the TableBlockIndexUniqueIdentifier passed
   *
   * @param blockIndexUniqueIdentifierWrapper
   * @return
   * @throws IOException
   * @throws MemoryException
   */
  public static BlockletDataMapIndexWrapper loadDataMap(
      TableBlockIndexUniqueIdentifierWrapper blockIndexUniqueIdentifierWrapper)
      throws IOException, MemoryException {
    TableBlockIndexUniqueIdentifier blockIndexUniqueIdentifier =
        blockIndexUniqueIdentifierWrapper.getTableBlockIndexUniqueIdentifier();
    List<BlockletDataMap> dataMaps = new ArrayList<>();
    if (null != blockIndexUniqueIdentifier) {
      try {
        SegmentIndexFileStore indexFileStore = new SegmentIndexFileStore();
        Set<String> filesRead = new HashSet<>();
        String segmentFilePath = blockIndexUniqueIdentifier.getIndexFilePath();
        Map<String, BlockMetaInfo> carbonDataFileBlockMetaInfoMapping = BlockletDataMapUtil
            .createCarbonDataFileBlockMetaInfoMapping(segmentFilePath);
        // if the indexUniqueIdentifier is not a merge index file
        // then we can directly get the datamaps for that
        if (blockIndexUniqueIdentifier.getMergeIndexFileName() == null) {
          Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil
              .getBlockMetaInfoMap(blockIndexUniqueIdentifierWrapper, indexFileStore, filesRead,
                  carbonDataFileBlockMetaInfoMapping);
          dataMaps.add(loadAndGetDataMap(blockIndexUniqueIdentifierWrapper, indexFileStore,
              blockMetaInfoMap));
        } else {
          // if the identifier is a merge index file then get the index files inside it
          // and then get the datamaps
          List<TableBlockIndexUniqueIdentifier> tableBlockIndexUniqueIdentifiers =
              BlockletDataMapUtil
                  .getIndexFileIdentifiersFromMergeFile(blockIndexUniqueIdentifier, indexFileStore);
          List<TableBlockIndexUniqueIdentifierWrapper> tableBlockIndexUniqueIdentifiersWrapper =
              new ArrayList<>(tableBlockIndexUniqueIdentifiers.size());
          for (TableBlockIndexUniqueIdentifier
                   tableBlockIndexUniqueIdentifier : tableBlockIndexUniqueIdentifiers) {
            tableBlockIndexUniqueIdentifiersWrapper.add(
                new TableBlockIndexUniqueIdentifierWrapper(tableBlockIndexUniqueIdentifier,
                    blockIndexUniqueIdentifierWrapper.getCarbonTable()));
          }
          for (TableBlockIndexUniqueIdentifierWrapper
                   indexUniqueIdentifierWrapper : tableBlockIndexUniqueIdentifiersWrapper) {
            Map<String, BlockMetaInfo> blockMetaInfoMap = BlockletDataMapUtil
                .getBlockMetaInfoMap(indexUniqueIdentifierWrapper, indexFileStore, filesRead,
                    carbonDataFileBlockMetaInfoMapping);
            dataMaps.add(loadAndGetDataMap(indexUniqueIdentifierWrapper, indexFileStore,
                blockMetaInfoMap));
          }
        }
      } catch (Throwable e) {
        for (DataMap dataMap : dataMaps) {
          dataMap.clear();
        }
        throw new IOException("Problem in loading segment blocks.", e);
      }
    }
    return new BlockletDataMapIndexWrapper(dataMaps);
  }

  /**
   * This method initializes and loads the blocklet dataMap
   *
   * @param identifierWrapper
   * @param indexFileStore
   * @param blockMetaInfoMap
   * @return
   * @throws IOException
   * @throws MemoryException
   */
  private static BlockletDataMap loadAndGetDataMap(
      TableBlockIndexUniqueIdentifierWrapper identifierWrapper,
      SegmentIndexFileStore indexFileStore, Map<String, BlockMetaInfo> blockMetaInfoMap)
      throws IOException, MemoryException {
    TableBlockIndexUniqueIdentifier identifier =
        identifierWrapper.getTableBlockIndexUniqueIdentifier();
    BlockletDataMap dataMap = new BlockletDataMap();
    dataMap.init(new BlockletDataMapModel(
        identifier.getIndexFilePath() + CarbonCommonConstants.FILE_SEPARATOR + identifier
            .getIndexFileName(), indexFileStore.getFileData(identifier.getIndexFileName()),
        blockMetaInfoMap, identifier.getSegmentId(), false));
    return dataMap;
  }
}