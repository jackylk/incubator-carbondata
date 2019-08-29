package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.metadata.TableReference;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class ExecuteEngine {

    /**
     * 启动执行引擎。</br>
     * 可在该方法中完成引擎的初始化动作。
     * @throws IllegalStateException 当引擎已启动，再次调用该方法将抛出该异常。
     */
    public abstract void startup();

    public abstract void shutdown();

    @Nullable
    public abstract FunctionManager getFunctionManager(ExecuteContext context) throws ExecuteException;

    public TableReference getTableReference(final ExecuteContext context, final Identifier tableName)
        throws ExecuteException {
        return this.getTableReference(context, context.getSchemaName(), tableName);
    }

    public abstract TableReference getTableReference(ExecuteContext context, Identifier schemaName,
        Identifier tableName) throws ExecuteException;

    @Nonnull
    public abstract <TExecuteResult> ExecutePlan<? extends TExecuteResult> generateExecutePlan(ExecuteContext context,
        Statement<TExecuteResult> statement) throws ExecuteException;

}
