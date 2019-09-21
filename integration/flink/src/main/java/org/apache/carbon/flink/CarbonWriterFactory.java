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
package org.apache.carbon.flink;

import java.io.IOException;
import java.util.Properties;

import org.apache.carbondata.core.metadata.converter.ThriftWrapperSchemaConverterImpl;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.path.CarbonTablePath;
import org.apache.flink.api.common.serialization.BulkWriter;
import org.apache.flink.core.fs.FSDataOutputStream;

/**
 * Note: All fields in implement class should be serializable.
 */
public abstract class CarbonWriterFactory implements BulkWriter.Factory<String> {

    public CarbonWriterFactory(
        final String databaseName,
        final String tableName,
        final String tablePath,
        final Properties tableProperties,
        final Properties writerProperties,
        final Properties carbonProperties
    ) {
        if (tableName == null) {
            throw new IllegalArgumentException("Argument [tableName] is null.");
        }
        if (tablePath == null) {
            throw new IllegalArgumentException("Argument [tablePath] is null.");
        }
        if (tableProperties == null) {
            throw new IllegalArgumentException("Argument [tableProperties] is null.");
        }
        if (writerProperties == null) {
            throw new IllegalArgumentException("Argument [writerProperties] is null.");
        }
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.tablePath = tablePath;
        this.tableProperties = tableProperties;
        this.writerProperties = writerProperties;
        this.carbonProperties = carbonProperties;
    }

    protected final String databaseName;

    protected final String tableName;

    private final String tablePath;

    private final Properties tableProperties;

    private final Properties writerProperties;

    private final Properties carbonProperties;

    @Override
    public CarbonWriter create(final FSDataOutputStream outputStream) throws IOException {
        if (!(outputStream instanceof ProxyRecoverableOutputStream)) {
            throw new IllegalArgumentException("Only support " + ProxyRecoverableOutputStream.class.getName() + ".");
        }
        this.setCarbonProperties();
        final CarbonTable table = this.getTable(this.writerProperties);
        final CarbonWriter writer = this.create(table, this.tableProperties, this.writerProperties);
        ((ProxyRecoverableOutputStream) outputStream).setWriter(writer);
        return writer;
    }

    protected abstract CarbonWriter create(CarbonTable table, Properties tableProperties, Properties writerProperties) throws IOException;

    protected CarbonTable getTable(final Properties writerProperties) throws IOException {
        return CarbonTable.buildFromTablePath(this.tableName, this.databaseName, this.tablePath, null);
    }

    private void setCarbonProperties() {
        final CarbonProperties carbonProperties = CarbonProperties.getInstance();
        for (String propertyName : this.carbonProperties.stringPropertyNames()) {
            carbonProperties.addProperty(propertyName, this.carbonProperties.getProperty(propertyName));
        }
    }

}
