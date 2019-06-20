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

package org.apache.spark.sql.leo;

import java.io.IOException;
import java.util.List;

import org.apache.carbondata.common.annotations.InterfaceAudience;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchemaStorageProvider;
import org.apache.carbondata.core.metadata.schema.table.DiskBasedDMSchemaStorageProvider;
import org.apache.carbondata.core.util.CarbonProperties;

/**
 * It maintains all the Model's in it.
 */
@InterfaceAudience.Internal
public final class ModelStoreManager {

  private final static ModelStoreManager instance = new ModelStoreManager();

  private DataMapSchemaStorageProvider provider = new DiskBasedDMSchemaStorageProvider(
      CarbonProperties.getInstance().getSystemFolderLocation() + "/model");

  /**
   * It gives all model schemas of a given table.
   *
   */
  public List<DataMapSchema> getDataMapSchemasOfTable(CarbonTable carbonTable) throws IOException {
    return provider.retrieveSchemas(carbonTable);
  }

  /**
   * It gives all model schemas from store.
   */
  public List<DataMapSchema> getAllModelSchemas() throws IOException {
    return provider.retrieveAllSchemas();
  }

  public DataMapSchema getModelSchema(String modelName)
      throws NoSuchDataMapException, IOException {
    return provider.retrieveSchema(modelName);
  }

  /**
   * Saves the model schema to storage
   * @param modelSchema
   */
  public void saveModelSchema(DataMapSchema modelSchema) throws IOException {
    provider.saveSchema(modelSchema);
  }

  /**
   * Drops the model schema from storage
   * @param modelName
   */
  public void dropModelSchema(String modelName) throws IOException {
    provider.dropSchema(modelName);
  }

  /**
   * Returns the singleton instance
   * @return
   */
  public static ModelStoreManager getInstance() {
    return instance;
  }
}
