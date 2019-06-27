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
public final class ExperimentStoreManager {

  private final static ExperimentStoreManager instance = new ExperimentStoreManager();

  private DataMapSchemaStorageProvider provider = new DiskBasedDMSchemaStorageProvider(
      CarbonProperties.getInstance().getSystemFolderLocation() + "/model");

  /**
   * It gives all experiment schemas of a given table.
   *
   */
  public List<DataMapSchema> getDataMapSchemasOfTable(CarbonTable carbonTable) throws IOException {
    return provider.retrieveSchemas(carbonTable);
  }

  /**
   * It gives all experiment schemas from store.
   */
  public List<DataMapSchema> getAllExperimentSchemas() throws IOException {
    return provider.retrieveAllSchemas();
  }

  public DataMapSchema getExperimentSchema(String experimentName)
      throws NoSuchDataMapException, IOException {
    return provider.retrieveSchema(experimentName);
  }

  /**
   * Saves the experiment schema to storage
   * @param experimentSchema
   */
  public void saveExperimentSchema(DataMapSchema experimentSchema) throws IOException {
    provider.saveSchema(experimentSchema);
  }

  /**
   * Drops the experiment schema from storage
   * @param experimentName
   */
  public void dropExperimentSchema(String experimentName) throws IOException {
    provider.dropSchema(experimentName);
  }

  /**
   * Returns the singleton instance
   * @return
   */
  public static ExperimentStoreManager getInstance() {
    return instance;
  }
}
