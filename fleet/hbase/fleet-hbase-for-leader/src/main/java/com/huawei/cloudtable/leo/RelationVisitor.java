package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.Relation;
import com.huawei.cloudtable.leo.relations.*;

public abstract class RelationVisitor<TResult, TParameter> {

  public abstract TResult returnDefault(Relation relation, TParameter parameter);

  public TResult visit(final LogicalAggregate aggregate, TParameter parameter) {
    return this.returnDefault(aggregate, parameter);
  }

  public TResult visit(final LogicalFilter filter, TParameter parameter) {
    return this.returnDefault(filter, parameter);
  }

  public TResult visit(final LogicalScan scan, TParameter parameter) {
    return this.returnDefault(scan, parameter);
  }

  public TResult visit(final LogicalLimit limit, TParameter parameter) {
    return returnDefault(limit, parameter);
  }

  public TResult visit(final LogicalOffset offset, TParameter parameter) {
    return this.returnDefault(offset, parameter);
  }

  public TResult visit(final LogicalProject project, TParameter parameter) {
    return this.returnDefault(project, parameter);
  }

}
