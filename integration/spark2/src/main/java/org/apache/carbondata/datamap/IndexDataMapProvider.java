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

package org.apache.carbondata.datamap;

import org.apache.carbondata.common.exceptions.MetadataProcessException;
import org.apache.carbondata.common.exceptions.sql.MalformedDataMapCommandException;
import org.apache.carbondata.core.datamap.DataMapRegistry;
import org.apache.carbondata.core.datamap.DataMapStoreManager;
import org.apache.carbondata.core.datamap.dev.IndexDataMapFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.format.TableInfo;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.preaaggregate.PreAggregateUtil;

public class IndexDataMapProvider implements DataMapProvider {

  private TableInfo originalTableInfo;

  @Override
  public void init(CarbonTable mainTable, DataMapSchema dataMapSchema, String ctasSqlStatement,
      SparkSession sparkSession) throws MalformedDataMapCommandException {
    IndexDataMapFactory dataMapFactory = createIndexDataMapFactory(dataMapSchema);
    DataMapStoreManager.getInstance().registerDataMap(
        mainTable.getAbsoluteTableIdentifier(), dataMapSchema, dataMapFactory);
    originalTableInfo = PreAggregateUtil.updateMainTable(mainTable, dataMapSchema, sparkSession);
  }

  @Override
  public void deinit(CarbonTable mainTable, DataMapSchema dataMapSchema,
      SparkSession sparkSession) {
    PreAggregateUtil.updateSchemaInfo(mainTable, originalTableInfo, sparkSession);
    DataMapStoreManager.getInstance().clearDataMap(
        mainTable.getAbsoluteTableIdentifier(), dataMapSchema.getDataMapName());
  }

  @Override
  public void rebuild(SparkSession sparkSession) {
  }

  private IndexDataMapFactory createIndexDataMapFactory(DataMapSchema dataMapSchema)
      throws MalformedDataMapCommandException {
    IndexDataMapFactory dataMapFactory;
    try {
      // try to create DataMapProvider instance by taking providerName as class name
      Class<? extends IndexDataMapFactory> providerClass =
          (Class<? extends IndexDataMapFactory>) Class.forName(dataMapSchema.getProviderName());
      dataMapFactory = providerClass.newInstance();
    } catch (ClassNotFoundException e) {
      // try to create DataMapProvider instance by taking providerName as short name
      dataMapFactory = getDataMapFactoryByShortName(dataMapSchema.getProviderName());
    } catch (Throwable e) {
      throw new MetadataProcessException(
          "failed to create DataMapProvider '" + dataMapSchema.getProviderName() + "'", e);
    }
    return dataMapFactory;
  }

  private IndexDataMapFactory getDataMapFactoryByShortName(String providerName)
      throws MalformedDataMapCommandException {
    IndexDataMapFactory dataMapFactory;
    String className = DataMapRegistry.getDataMapClassName(providerName);
    if (className != null) {
      try {
        Class<? extends IndexDataMapFactory> datamapClass =
            (Class<? extends IndexDataMapFactory>) Class.forName(providerName);
        dataMapFactory = datamapClass.newInstance();
      } catch (ClassNotFoundException ex) {
        throw new MalformedDataMapCommandException(
            "DataMap '" + providerName + "' not found", ex);
      } catch (Throwable ex) {
        throw new MetadataProcessException(
            "failed to create DataMap '" + providerName + "'", ex);
      }
    } else {
      throw new MalformedDataMapCommandException(
          "DataMap '" + providerName + "' not found");
    }
    return dataMapFactory;
  }
}
