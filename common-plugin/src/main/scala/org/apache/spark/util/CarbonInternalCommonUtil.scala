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

package org.apache.spark.util

import org.apache.spark.SparkContext

import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.core.util.CarbonProperties
import org.apache.carbondata.spark.core.CarbonCommonPluginConstants
import org.apache.carbondata.spark.rdd.CarbonMergeFilesRDD


object CarbonInternalCommonUtil {

  /**
   * Merge the carbonindex files with in the segment to carbonindexmerge file inside same segment
   *
   * @param sparkContext
   * @param segmentIds
   * @param tablePath
   * @param carbonTable
   * @param mergeIndexProperty
   * @param readFileFooterFromCarbonDataFile flag to read file footer information from carbondata
   *                                         file. This will used in case of upgrade from version
   *                                         which do not store the blocklet info to current
   *                                         version
   */
  def mergeIndexFiles(sparkContext: SparkContext,
    segmentIds: Seq[String],
    tablePath: String,
    carbonTable: CarbonTable,
    mergeIndexProperty: Boolean,
    readFileFooterFromCarbonDataFile: Boolean = false): Unit = {
    if (mergeIndexProperty) {
      new CarbonMergeFilesRDD(
        sparkContext,
        carbonTable,
        segmentIds,
        carbonTable.isHivePartitionTable,
        readFileFooterFromCarbonDataFile).collect()
    } else {
      try {
        if (CarbonProperties.getInstance().getProperty(
          CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT,
          CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT).toBoolean) {
          new CarbonMergeFilesRDD(
            sparkContext,
            carbonTable,
            segmentIds,
            carbonTable.isHivePartitionTable,
            readFileFooterFromCarbonDataFile).collect()
        }
      } catch {
        case _: Exception =>
          if (CarbonCommonPluginConstants.CARBON_MERGE_INDEX_IN_SEGMENT_DEFAULT.toBoolean) {
            new CarbonMergeFilesRDD(
              sparkContext,
              carbonTable,
              segmentIds,
              carbonTable.isHivePartitionTable,
              readFileFooterFromCarbonDataFile).collect()
          }
      }
    }
  }
}
