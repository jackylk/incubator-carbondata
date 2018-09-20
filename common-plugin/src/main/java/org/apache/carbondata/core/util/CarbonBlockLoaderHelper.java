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

package org.apache.carbondata.core.util;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArraySet;

import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;

/**
 * The class provides the map of table blocks already submitted for DataMap load
 */
public class CarbonBlockLoaderHelper {

  private static final CarbonBlockLoaderHelper carbonBlockLoaderHelper =
      new CarbonBlockLoaderHelper();
  /**
   * maintains the map of segments already considered for the btree load
   */
  private ConcurrentMap<AbsoluteTableIdentifier, CopyOnWriteArraySet<String>> tableBlockMap;

  private CarbonBlockLoaderHelper() {
    tableBlockMap = new ConcurrentHashMap<>();
  }

  /**
   * return's instance of the CarbonBlockLoaderHelper
   *
   * @return
   */
  public static CarbonBlockLoaderHelper getInstance() {
    return carbonBlockLoaderHelper;
  }

  private Set<String> getTableblocks(AbsoluteTableIdentifier absoluteTableIdentifier) {
    CopyOnWriteArraySet<String> blockSet = tableBlockMap.get(absoluteTableIdentifier);
    if (null == blockSet) {
      CopyOnWriteArraySet<String> newBlockSet = new CopyOnWriteArraySet<String>();
      blockSet = tableBlockMap.putIfAbsent(absoluteTableIdentifier, newBlockSet);
      if (null == blockSet) {
        blockSet = newBlockSet;
      }
    }
    return blockSet;
  }

  /**
   * The method check the tableBlockMap to know weather the block is already submitted/ considered
   * for the DataMap loading.
   *
   * @param uniqueBlockId <String> Uniquely identify the block
   * @return <false> if uniqueSegmentId is mapped to any of the key present in the
   * segmentsMap map else <true>
   */
  public Boolean checkAlreadySubmittedBlock(final AbsoluteTableIdentifier absoluteTableIdentifier,
      final String uniqueBlockId) {
    Set<String> tableBlocks = getTableblocks(absoluteTableIdentifier);
    // tableBlocks is a type of CopyOnWriteArraySet, so avoided taking lock during write/add
    return tableBlocks.add(uniqueBlockId);
  }

  /**
   * This api is used to clear the tableBlockMap so that if there is cache mis then the
   * the table blocks should be considered as not already submitted for the DataMap load.
   *
   * @param absoluteTableIdentifier Identifies table uniquely
   * @param uniqueBlockId           Set<String> Set of blockId
   */
  public void clear(final AbsoluteTableIdentifier absoluteTableIdentifier,
      final Set<String> uniqueBlockId) {
    CopyOnWriteArraySet<String> blockSet = tableBlockMap.get(absoluteTableIdentifier);
    if (null != blockSet) {
      for (String block : uniqueBlockId) {
        blockSet.remove(block);
      }
      if (blockSet.isEmpty()) {
        tableBlockMap.remove(absoluteTableIdentifier);
      }
    }
  }
}
