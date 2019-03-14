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

package org.apache.spark.sql.events

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.spark.sql.CarbonEnv
import org.apache.spark.util.CarbonInternalScalaUtil

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.cache.CacheProvider
import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.indexstore.BlockletDataMapIndexWrapper
import org.apache.carbondata.datamap.bloom.BloomCacheKeyValue
import org.apache.carbondata.events.{Event, OperationContext, OperationEventListener, ShowTableCacheEvent}
import org.apache.carbondata.spark.core.metadata.IndexMetadata


class SIShowCacheEventListener extends OperationEventListener {
  val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)

  /**
   * Called on a specified event occurrence
   *
   * @param event
   */
  override def onEvent(event: Event, operationContext: OperationContext): Unit = {
    event match {
      case showTableCacheEvent: ShowTableCacheEvent =>

        val carbonTable = showTableCacheEvent.carbonTable
        val sparkSession = showTableCacheEvent.sparkSession
        if (carbonTable.isChildDataMap || CarbonInternalScalaUtil.isIndexTable(carbonTable)) {
          throw new UnsupportedOperationException("Operation not allowed on child table.")
        }
        val reverseSIToMainTablePathMap = mutable.Map[String, String]()
        val parentTablePath = carbonTable.getTablePath + CarbonCommonConstants.FILE_SEPARATOR
        val currentTableSizeMap = operationContext.getProperty(parentTablePath)
          .asInstanceOf[mutable.Map[String, (String, String, Long, Long)]]
        val indexMetadata = IndexMetadata
          .deserialize(carbonTable.getTableInfo.getFactTable.getTableProperties
            .get(carbonTable.getCarbonTableIdentifier.getTableId))
        if (null != indexMetadata) {
          val indexTables = indexMetadata.getIndexTables.asScala
          // if there are no index tables for a given fact table do not perform any action
          if (indexTables.nonEmpty) {
            indexTables.foreach(indexTable => {
              val indexTablePath = CarbonEnv.getCarbonTable(
                Some(carbonTable.getDatabaseName), indexTable)(sparkSession).getTablePath +
                                   CarbonCommonConstants.FILE_SEPARATOR
              currentTableSizeMap.put(indexTablePath, (indexTable, "secondary index", 0L, 0L))
              reverseSIToMainTablePathMap.put(indexTablePath, parentTablePath)
            })
          }
          fetchChildrenTableInfoFromCache(reverseSIToMainTablePathMap, operationContext)
        }
    }
  }

  /**
   * For all the SI-tables' paths in reverseDatamapPathToTablePathMap
   * fetches the information from cache and puts that in operationContext
   *
   * @param reverseDatamapPathToTablePathMap
   * @param operationContext
   */
  def fetchChildrenTableInfoFromCache(reverseDatamapPathToTablePathMap: mutable.Map[String, String],
    operationContext: OperationContext): Unit = {

    CacheProvider.getInstance().getCarbonCache.getCacheMap.asScala.foreach {
      case (key, cacheable) =>
        cacheable match {
          case _: BlockletDataMapIndexWrapper =>
            val childTablePath = reverseDatamapPathToTablePathMap.find {
              case (childPath, _) =>
                key.startsWith(childPath)
            }
            if (childTablePath.isDefined) {
              val (childPath, parentPath) = childTablePath.get
              val currentTableSizeMap = operationContext.getProperty(parentPath)
                .asInstanceOf[mutable.Map[String, (String, String, Long, Long)]]
              val (name, provider, indexSize, dmSize) = currentTableSizeMap(childPath)
              currentTableSizeMap.put(childPath,
                (name, provider, indexSize + cacheable.getMemorySize, dmSize))
            }
          case _: BloomCacheKeyValue.CacheValue =>
            val datamapPath = reverseDatamapPathToTablePathMap.find {
              case (childPath, _) =>
                key.startsWith(childPath)
            }
            if (datamapPath.isDefined) {
              val (childPath, parentPath) = datamapPath.get
              val currentTableSizeMap = operationContext.getProperty(parentPath)
                .asInstanceOf[mutable.Map[String, (String, String, Long, Long)]]
              val (name, provider, indexSize, dmSize) = currentTableSizeMap(childPath)
              currentTableSizeMap.put(childPath,
                (name, provider, indexSize, dmSize + cacheable.getMemorySize))
            }
          case _ =>
        }
    }

  }

}
