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

package org.apache.carbondata.spark.spark.indextable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.spark.spark.secondaryindex.exception.IndexTableExistException;

import com.google.gson.Gson;

public class IndexTableUtil {

  /**
   * convert string to index table info object
   *
   * @param gsonData
   * @return
   */
  public static IndexTableInfo[] fromGson(String gsonData) {
    Gson gson = new Gson();
    return gson.fromJson(gsonData, IndexTableInfo[].class);
  }

  /**
   * converts index table info object to string
   *
   * @param gsonData
   * @return
   */
  public static String toGson(IndexTableInfo[] gsonData) {
    Gson gson = new Gson();
    return gson.toJson(gsonData);
  }

  /**
   * adds index table info into parent table properties
   *
   * @param gsonData
   * @param newIndexTable
   * @return
   * @throws IndexTableExistException
   */
  public static String checkAndAddIndexTable(String gsonData, IndexTableInfo newIndexTable)
      throws IndexTableExistException {
    IndexTableInfo[] indexTableInfos = fromGson(gsonData);
    if (null == indexTableInfos) {
      indexTableInfos = new IndexTableInfo[0];
    }
    List<IndexTableInfo> indexTables =
        new ArrayList<IndexTableInfo>(Arrays.asList(indexTableInfos));
    for (IndexTableInfo indexTable : indexTableInfos) {
      if (indexTable.getIndexCols().size() == newIndexTable.getIndexCols().size()) {
        //If column order is not same, then also index table creation should be successful
        //eg., index1 is a,b,d and index2 is a,d,b. Both table creation should be successful
        boolean isColumnOrderSame = true;
        for (int i = 0; i < indexTable.getIndexCols().size(); i++) {
          if (!indexTable.getIndexCols().get(i)
              .equalsIgnoreCase(newIndexTable.getIndexCols().get(i))) {
            isColumnOrderSame = false;
          }
        }
        if (isColumnOrderSame) {
          throw new IndexTableExistException("Index Table with selected columns already exist");
        }
      }
    }
    indexTables.add(newIndexTable);
    return toGson(indexTables.toArray(new IndexTableInfo[indexTables.size()]));
  }

  /**
   * removes index table info from parent table properties
   *
   * @param gsonData
   * @param dbName
   * @param tableName
   * @return
   */
  public static String removeIndexTable(String gsonData, String dbName, String tableName) {
    IndexTableInfo[] indexTableInfos = fromGson(gsonData);
    if (null == indexTableInfos) {
      indexTableInfos = new IndexTableInfo[0];
    }
    List<IndexTableInfo> indexTables =
        new ArrayList<IndexTableInfo>(Arrays.asList(indexTableInfos));
    for (IndexTableInfo indexTable : indexTableInfos) {
      if (indexTable.getDatabaseName().equals(dbName) && indexTable.getTableName()
          .equals(tableName)) {
        indexTables.remove(indexTable);
      }
    }
    return toGson(indexTables.toArray(new IndexTableInfo[indexTables.size()]));
  }
}
