package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.statement.DataManipulationStatement;
import com.huawei.cloudtable.leo.statement.DataQueryStatement;

public interface StatementVisitor<TResult, TParameter> {

  TResult visit(DataManipulationStatement statement, TParameter parameter);

  TResult visit(DataQueryStatement statement, TParameter parameter);

}
