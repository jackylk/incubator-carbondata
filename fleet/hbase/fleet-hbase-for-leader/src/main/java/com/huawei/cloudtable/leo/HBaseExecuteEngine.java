
package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.FunctionManager;
import com.huawei.cloudtable.leo.metadata.TableReference;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;

public final class HBaseExecuteEngine extends ExecuteEngine {

    private enum State {

        CREATED,

        RUNNING,

        STOPPED

    }

    HBaseExecuteEngine(final Configuration configuration, final HBaseConnectionFactory connectionFactory,
        final HBaseMetadataProvider metadataProvider) {
        this.configuration = configuration;
        this.connectionFactory = connectionFactory;
        this.metadataProvider = metadataProvider;
        this.state = State.CREATED;
    }

    private final Configuration configuration;

    private final HBaseConnectionFactory connectionFactory;

    private final HBaseMetadataProvider metadataProvider;

    private volatile State state;

    private Connection connection;

    private HBaseMetadataManager metadataManager;

    @Override
    public void startup() {
        synchronized (this) {
            switch (this.state) {
                case CREATED:
                    break;
                case RUNNING:
                    throw new IllegalStateException("Engine is running.");
                case STOPPED:
                    throw new IllegalStateException("Engine is stopped.");
                default:
                    throw new RuntimeException();
            }
            final Connection connection;
            try {
                connection = this.connectionFactory.newConnection(this.configuration);
            } catch (IOException exception) {
                // TODO
                throw new UnsupportedOperationException(exception);
            }
            this.metadataManager = new HBaseMetadataManager(this.metadataProvider);
            this.connection = connection;
            this.state = State.RUNNING;
        }
    }

    @Override
    public void shutdown() {
        synchronized (this) {
            switch (this.state) {
                case CREATED:
                    throw new IllegalStateException("Engine is not running.");
                case RUNNING:
                    break;
                case STOPPED:
                    throw new IllegalStateException("Engine is stopped.");
                default:
                    throw new RuntimeException();
            }
            final Connection connection = this.connection;
            this.state = State.STOPPED;
            this.connection = null;
            this.metadataManager = null;
            try {
                connection.close();
            } catch (IOException exception) {
                // TODO
                throw new UnsupportedOperationException(exception);
            }
        }
    }

    public Connection getConnection() {
        Connection connection = this.connection;
        if (connection == null) {
            synchronized (this) {
                connection = this.connection;
                if (connection == null) {
                    switch (this.state) {
                        case CREATED:
                            throw new IllegalStateException("Engine is not running.");
                        case STOPPED:
                            throw new IllegalStateException("Engine is stopped.");
                        default:
                            throw new RuntimeException();
                    }
                }
            }
        }
        return connection;
    }

    public HBaseMetadataManager getMetadataManager() {
        HBaseMetadataManager metadataManager = this.metadataManager;
        if (metadataManager == null) {
            synchronized (this) {
                metadataManager = this.metadataManager;
                if (metadataManager == null) {
                    switch (this.state) {
                        case CREATED:
                            throw new IllegalStateException("Engine is not running.");
                        case STOPPED:
                            throw new IllegalStateException("Engine is stopped.");
                        default:
                            throw new RuntimeException();
                    }
                }
            }
        }
        return metadataManager;
    }

    @Override
    public TableReference getTableReference(final ExecuteContext context, final Identifier schemaName,
        final Identifier tableName) {
        return this.metadataManager.getTableReference(context.getTenantIdentifier(), schemaName, tableName);
    }

    @Nullable
    @Override
    public FunctionManager getFunctionManager(ExecuteContext context) throws ExecuteException {
        // TODO
        return FunctionManager.BUILD_IN;
    }

    @Nonnull
    @Override
    public <TExecuteResult> ExecutePlan<? extends TExecuteResult> generateExecutePlan(final ExecuteContext context,
        final Statement<TExecuteResult> statement) {
        final HBaseExecutePlanner<TExecuteResult, Statement<TExecuteResult>> planner =
            HBaseExecutePlanners.getPlanner(statement);
        if (planner == null) {
            // TODO
            throw new UnsupportedOperationException();
        }
        return planner.plan(this, context, statement);
    }

}
