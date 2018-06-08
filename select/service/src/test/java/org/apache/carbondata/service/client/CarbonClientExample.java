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

package org.apache.carbondata.service.client;

import java.io.IOException;

import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.vision.algorithm.Algorithm;
import org.apache.carbondata.vision.common.VisionConfiguration;
import org.apache.carbondata.vision.common.VisionException;
import org.apache.carbondata.vision.model.Model;
import org.apache.carbondata.vision.predict.PredictContext;
import org.apache.carbondata.vision.table.Record;
import org.apache.carbondata.vision.table.Table;

public class CarbonClientExample {

  public static void main(String[] args) throws VisionException, IOException {

    if (args.length != 4) {
      System.err.println(
          "Usage: CarbonClientExample <log4j> <model path> <result.bin> <properties file>");
      return;
    }

    ExampleUtils.initLog4j(args[0]);

    // start client
    CarbonClient client = createClient(args[3]);

    // load library
    boolean isSuccess = client.loadLibrary("carbonvision");
    if (!isSuccess) {
      throw new VisionException("Failed to load library");
    }

    // load model
    Model model = client.loadModel(args[1]);

    // cache table
    Table table = new Table("default", "frs_table", "feature");
    CarbonTable carbonTable = client.cacheTable(table);
    if (table == null) {
      throw new VisionException("can not found the table: " + table.getPresentName());
    }

    Table table1 = new Table("default", "frs_table1", "feature");
    CarbonTable carbonTable1 = client.cacheTable(table1);
    if (table1 == null) {
      throw new VisionException("can not found the table: " + table1.getPresentName());
    }

    Table table2 = new Table("default", "frs_table2", "feature");
    CarbonTable carbonTable2 = client.cacheTable(table2);
    if (table2 == null) {
      throw new VisionException("can not found the table: " + table2.getPresentName());
    }

    // choose algorithm
    Algorithm algorithm =
        new Algorithm("org.apache.carbondata.vision.algorithm.impl.KNNSearch", "1.0");

    // create PredictContext
    String featureSetFile = args[2];
    byte[] searchFeature = ExampleUtils.generateFeatureSetExample(featureSetFile, 1, 0);

    PredictContext context = PredictContext
        .builder()
        .algorithm(algorithm)
        .model(model)
        .table(table)
        .conf(VisionConfiguration.SELECT_SEARCH_VECTOR, searchFeature)
        .conf(VisionConfiguration.SELECT_TOP_N, 10)
        .conf(VisionConfiguration.SELECT_VECTOR_SIZE, 288)
        .conf(VisionConfiguration.SELECT_PROJECTION, new String[] { "id" })
        .conf(VisionConfiguration.SELECT_BATCH_SIZE, 100000)
        .create();

    // search
    Record[] result = client.search(context);
    ExampleUtils.printRecords(result);

    context.setTable(table1);
    result = client.search(context);
    ExampleUtils.printRecords(result);

    context.setTable(table2);
    result = client.search(context);
    ExampleUtils.printRecords(result);

  }

  public static CarbonClient createClient(String filePath) throws VisionException {
    VisionConfiguration conf = new VisionConfiguration();
    conf.load(filePath);
    CarbonClient client = new CarbonClient(conf);
    client.init();
    return client;
  }
}
