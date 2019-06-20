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

package org.apache.carbondata;

import java.io.File;
import java.io.IOException;

import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.util.CarbonProperties;

import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.CarbonSession;
import org.apache.spark.sql.SparkSession;

public class TestCreateModel {

  public static void main(String[] args) throws IOException {
    String warehouse = new File("./warehouse").getCanonicalPath();
    // create leo session
    SparkSession.Builder builder = SparkSession.builder().master("local").appName("TestCreateModel")
        .config("spark.carbon.sessionstate.classname",
            "org.apache.spark.sql.leo.LeoSessionStateBuilder")
        .config("spark.driver.host", "localhost").config("spark.sql.warehouse.dir", warehouse);

    SparkSession carbon = new CarbonSession.CarbonBuilder(builder).getOrCreateCarbonSession();

    testModel(carbon);
    FileUtils.deleteDirectory(new File(warehouse));
    carbon.close();
  }

  private static void testModel(SparkSession carbon) throws IOException {
    carbon.sql("drop table if exists default.test");
    carbon.sql("create table default.test(c1 int, c2 int, c3 int) stored by 'carbondata'");
    carbon.sql("drop model if exists default.m1");
    // create model without options
    carbon.sql("CREATE MODEL default.m1  as select c1,c2 as a from default.test where c3>5 and c2=1");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    carbon.sql("drop model if exists default.m1");
    assert (!FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    carbon.sql("drop model if exists default.m2");
    // create model with options
    carbon.sql(
        "CREATE MODEL if not exists default.m2 OPTIONS('label_col'='c2', 'max_iteration'='100') as select c1,c2 from default.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m2.dmschema"));
    carbon.sql("drop model if exists default.m2");
    carbon.sql("drop table if exists default.test");

  }
}