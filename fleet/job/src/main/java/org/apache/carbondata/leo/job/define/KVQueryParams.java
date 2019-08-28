package org.apache.carbondata.leo.job.define;

import org.apache.spark.sql.catalyst.expressions.Expression;

public class KVQueryParams {

  private String databaseName;
  private String tableName;
  private String[] selectColumns;
  private Expression filterExpr;
  private long limit;

  public KVQueryParams(String databaseName, String tableName, String[] select,
      Expression filterExpr) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.selectColumns = select;
    this.filterExpr = filterExpr;
    this.limit = Long.MAX_VALUE;
  }

  public KVQueryParams(String databaseName, String tableName, String[] select,
      Expression filterExpr, long limit) {
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.selectColumns = select;
    this.filterExpr = filterExpr;
    this.limit = limit;
  }

  public String getDatabaseName() {
    return databaseName;
  }

  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public String[] getSelect() {
    return selectColumns;
  }

  public void setSelect(String[] selectColumns) {
    this.selectColumns = selectColumns;
  }

  public Expression getFilterExpr() {
    return filterExpr;
  }

  public void setFilterExpr(Expression filterExpr) {
    this.filterExpr = filterExpr;
  }

  public long getLimit() {
    return limit;
  }

  public void setLimit(long limit) {
    this.limit = limit;
  }
}
