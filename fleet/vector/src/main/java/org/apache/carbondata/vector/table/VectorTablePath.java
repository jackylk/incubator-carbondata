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

package org.apache.carbondata.vector.table;

import java.io.File;

import org.apache.carbondata.core.metadata.schema.table.column.CarbonColumn;

/**
 * file layout and file name
 */
public class VectorTablePath {

  /**
   * name of column data file
   * @param column
   * @return
   */
  public static String getColumnFileName(CarbonColumn column) {
    return column.getColName() + ".array";

  }

  /**
   * offset of each value in column data
   * @param column
   * @return
   */
  public static String getOffsetFileName(CarbonColumn column) {
    return column.getColName() + ".offset";
  }

  /**
   * file path of column file
   * @param folderPath
   * @param column
   * @return
   */
  public static String getColumnFilePath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getColumnFileName(column);
  }

  /**
   * file path of offset file
   * @param folderPath
   * @param column
   * @return
   */
  public static String getOffsetFilePath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getOffsetFileName(column);
  }


  /**
   * name of column data file
   * @param column
   * @return
   */
  public static String getColumnFolderName(CarbonColumn column) {
    return column.getColName();

  }

  public static String getComplexFolderPath(String folderPath, CarbonColumn column) {
    return folderPath + File.separator + getColumnFolderName(column);
  }

}
