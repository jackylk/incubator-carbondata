
package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.analyzer.SemanticAnalyseContext;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyzer;
import com.huawei.cloudtable.leo.analyzer.SemanticAnalyzers;
import com.huawei.cloudtable.leo.analyzer.SemanticException;
import com.huawei.cloudtable.leo.language.SQLStatement;
import com.huawei.cloudtable.leo.language.SyntaxAnalyzer;
import com.huawei.cloudtable.leo.language.SyntaxException;
import com.huawei.cloudtable.leo.statement.DataManipulationStatement;
import com.huawei.cloudtable.leo.statement.DataQueryStatement;
import org.apache.hadoop.conf.Configuration;

import javax.annotation.Nonnull;

public class HBaseQueryExecutor {

    public HBaseQueryExecutor(final Configuration configuration, final HBaseMetadataProvider metadataProvider) {
        this.executeEngine = new HBaseExecuteEngine(configuration, HBaseConnectionFactory.BUILD_IN, metadataProvider);
        this.executeEngine.startup();
    }

    public HBaseQueryExecutor(final Configuration configuration, final HBaseConnectionFactory connectionFactory,
        final HBaseMetadataProvider metadataProvider) {
        this.executeEngine = new HBaseExecuteEngine(configuration, connectionFactory, metadataProvider);
        this.executeEngine.startup();
    }

    private final ExecuteEngine executeEngine;

    private final SyntaxAnalyzer syntaxAnalyzer = SyntaxAnalyzerBuilder.build();

    public ResultSet executeQuery(final String sql) {
        final String tenantIdentifier = "";
        final Identifier schemaName = Identifier.of("test");
        final ExecuteContext executeContext = new ExecuteContext() {
            @Nonnull
            @Override
            public String getTenantIdentifier() {
                return tenantIdentifier;
            }

            @Nonnull
            @Override
            public Identifier getSchemaName() {
                return schemaName;
            }
        };
        final Statement statement = this.compile(executeContext, sql);
        if (!(statement instanceof DataQueryStatement)) {
            // TODO 抛异常
            throw new UnsupportedOperationException();
        }
        return this.executeQuery(executeContext, (DataQueryStatement) statement);
    }

    private ResultSet executeQuery(final ExecuteContext executeContext, final DataQueryStatement statement) {
        final Table executeResult;
        try {
            executeResult = this.executeEngine.generateExecutePlan(executeContext, statement).execute();
        } catch (ExecuteException exception) {
            // TODO
            throw new UnsupportedOperationException();
        }
        return new ResultSet(executeResult, statement.getResultAttributeTitles());
    }

    public int executeUpdate(final String sql) {
        final String tenantIdentifier = "";
        final Identifier schemaName = Identifier.of("test");
        final ExecuteContext executeContext = new ExecuteContext() {
            @Nonnull
            @Override
            public String getTenantIdentifier() {
                return tenantIdentifier;
            }

            @Nonnull
            @Override
            public Identifier getSchemaName() {
                return schemaName;
            }
        };
        final Statement statement = this.compile(executeContext, sql);
        if (!(statement instanceof DataManipulationStatement)) {
            // TODO 抛异常
            throw new UnsupportedOperationException();
        }
        return this.executeUpdate(executeContext, (DataManipulationStatement) statement);
    }

    private int executeUpdate(final ExecuteContext executeContext, final DataManipulationStatement statement) {
        final ExecuteMutations executeMutations;
        try {
            executeMutations = this.executeEngine.generateExecutePlan(executeContext, statement).execute();
        } catch (ExecuteException exception) {
            // TODO
            throw new UnsupportedOperationException();
        }
        try {
            executeMutations.commit();
        } catch (ExecuteException exception) {
            // TODO
            throw new UnsupportedOperationException(exception);
        }
        return executeMutations.getRowCount();
    }

    @SuppressWarnings("unchecked")
    private Statement compile(final ExecuteContext executeContext, final String sql) {
        try {
            final SQLStatement statement = this.syntaxAnalyzer.analyse(sql, SQLStatement.class).getRoot();
            final SemanticAnalyzer semanticAnalyzer = SemanticAnalyzers.getSemanticAnalyzer(statement.getClass());
            if (semanticAnalyzer == null) {
                // TODO
                throw new UnsupportedOperationException();
            }
            final SemanticAnalyseContext semanticAnalyseContext =
                SemanticAnalyseContext.newDefault(sql, this.executeEngine, executeContext);
            return semanticAnalyzer.analyse(semanticAnalyseContext, statement);
        } catch (SyntaxException | SemanticException | ExecuteException exception) {
            // TODO
            throw new UnsupportedOperationException(exception);
        }
    }

}
