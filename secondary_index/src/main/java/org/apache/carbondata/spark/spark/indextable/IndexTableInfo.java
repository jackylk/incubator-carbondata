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
package org.apache.carbondata.spark.spark.indextable;

import java.io.Serializable;
import java.util.List;

public class IndexTableInfo implements Serializable {

  private static final long serialVersionUID = 1106104914918491724L;

  private String databaseName;
  private String tableName;
  private List<String> indexCols;

  public IndexTableInfo(String databaseName, String tableName, List<String> indexCols) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.indexCols = indexCols;
  }

  /**
   * returns db name
   *
   * @return
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /**
   * returns table name
   *
   * @return
   */
  public String getTableName() {
    return tableName;
  }

  /**
   * returns all the index columns
   *
   * @return
   */
  public List<String> getIndexCols() {
    return indexCols;
  }

  /**
   * compares both the objects
   *
   * @param obj
   * @return
   */
  @Override public boolean equals(Object obj) {
    if (obj == null) {
      return false;

    }
    if (!(obj instanceof IndexTableInfo)) {
      return false;
    }
    IndexTableInfo other = (IndexTableInfo) obj;
    if (indexCols == null) {
      if (other.indexCols != null) {
        return false;
      }
    } else if (!indexCols.equals(other.indexCols)) {
      return false;
    }
    return true;
  }

  @Override public int hashCode() {
    int hashCode = 0;
    for (String s : indexCols) {
      hashCode += s.hashCode();
    }
    return hashCode;
  }
}
