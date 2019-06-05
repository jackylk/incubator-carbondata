package org.apache.carbondata.router;

import java.util.List;

import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.NamedExpression;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.datasources.LogicalRelation;

public class Destination {

  private QueryType queryType;

  public enum QueryType {HBASE, CARBON}

  private LogicalPlan analyzed;

  private boolean rewitten;

  private String rewrittenSql;

  private List<NamedExpression> columns;
  private Expression expr;
  private LogicalRelation relation;
  private Long maxRows;

  public Destination(QueryType queryType, LogicalPlan analyzed) {
    this.queryType = queryType;
    this.analyzed = analyzed;
  }

  public List<NamedExpression> getColumns() {
    return columns;
  }

  public void setColumns(List<NamedExpression> columns) {
    this.columns = columns;
  }

  public Expression getExpr() {
    return expr;
  }

  public void setExpr(Expression expr) {
    this.expr = expr;
  }

  public LogicalRelation getRelation() {
    return relation;
  }

  public void setRelation(LogicalRelation relation) {
    this.relation = relation;
  }

  public Long getMaxRows() {
    return maxRows;
  }

  public void setMaxRows(Long maxRows) {
    this.maxRows = maxRows;
  }

  public QueryType getQueryType() {
    return queryType;
  }

  public void setQueryType(QueryType queryType) {
    this.queryType = queryType;
  }

  public LogicalPlan getAnalyzed() {
    return analyzed;
  }

  public void setAnalyzed(LogicalPlan analyzed) {
    this.analyzed = analyzed;
  }

  public boolean isRewitten() {
    return rewitten;
  }

  public void setRewitten(boolean rewitten) {
    this.rewitten = rewitten;
  }

  public String getRewrittenSql() {
    return rewrittenSql;
  }

  public void setRewrittenSql(String rewrittenSql) {
    this.rewrittenSql = rewrittenSql;
  }
}
