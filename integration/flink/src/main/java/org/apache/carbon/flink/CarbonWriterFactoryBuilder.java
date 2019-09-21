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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.ServiceLoader;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.log4j.Logger;

public abstract class CarbonWriterFactoryBuilder {

    private static final Logger LOGGER = LogServiceFactory.getLogService(CarbonWriterFactoryBuilder.class.getCanonicalName());

    private static final Map<String, CarbonWriterFactoryBuilder> BUILDER_MAP;

    static {
        final Map<String, CarbonWriterFactoryBuilder> builderMap = new HashMap<>();
        final ServiceLoader<CarbonWriterFactoryBuilder> builderLoader = ServiceLoader.load(CarbonWriterFactoryBuilder.class);
        for (CarbonWriterFactoryBuilder builder : builderLoader) {
            try {
                builderMap.put(builder.getType(), builder);
                LOGGER.debug("Added carbon writer factory builder " + builder.getClass().getName());
            } catch (Throwable exception) {
                LOGGER.error("Failed carbon writer factory builder.", exception);
            }
        }
        BUILDER_MAP = builderMap;
    }

    /**
     * Get writer factory builder by type.
     *
     * @param type Only support 'S3' now.
     * @return Return the writer factory builder.
     */
    public static CarbonWriterFactoryBuilder get(final String type) {
        if (type == null) {
            throw new IllegalArgumentException("Argument [type] is null.");
        }
        CarbonWriterFactoryBuilder builder = BUILDER_MAP.get(type);
        if (builder == null) {
            if (type.equalsIgnoreCase(CarbonS3WriterFactoryBuilder.TYPE)) {
                return new CarbonS3WriterFactoryBuilder();
            }
        }
        return builder;
    }

    /**
     * Get the type of writer factory.
     *
     * @return Return the type of writer factory.
     */
    public abstract String getType();

    /**
     * Build writer factory.
     *
     * @param databaseName     The target carbon database name.
     * @param tableName        The target carbon table name.
     * @param tablePath        The target carbon table path, can get it from carbon table metadata.
     * @param tableProperties  The target carbon table properties.
     * @param writerProperties Writer properties.
     * @param carbonProperties Carbon environment properties.
     * @return Return the writer factory.
     */
    public abstract CarbonWriterFactory build(
        String databaseName,
        String tableName,
        String tablePath,
        Properties tableProperties,
        Properties writerProperties,
        Properties carbonProperties
    );

}
