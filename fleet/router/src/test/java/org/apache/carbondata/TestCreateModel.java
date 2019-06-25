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
import java.util.Arrays;

import org.apache.carbondata.ai.DataScan;
import org.apache.carbondata.common.exceptions.sql.NoSuchDataMapException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.table.DataMapSchema;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ObjectSerializationUtil;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.leo.LeoEnv;
import org.apache.spark.sql.leo.ModelStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit test for leo model ddl commands
 */
public class TestCreateModel {

  private static SparkSession carbon;

  @BeforeClass
  public static void setup() throws IOException {
    String warehouse = new File("./warehouse").getCanonicalPath();
    SparkSession.Builder builder = SparkSession.builder()
        .master("local")
        .config("spark.driver.host", "localhost")
        .config("spark.sql.warehouse.dir", warehouse);
    carbon = LeoEnv.getOrCreateLeoSession(builder);
    carbon.sql("drop database if exists db cascade");
    carbon.sql("create database db");
  }

  /**
   * test create and drop model
   */
  @Test
  public void testModel() throws IOException {
    carbon.sql("drop table if exists db.test");
    carbon.sql("create table db.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop model if exists db.m1");
    // create model without options
    carbon.sql("CREATE MODEL db.m1  as select c1,c2 as a from db.test where c3>5 and c2=1");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    carbon.sql("drop model if exists db.m1");
    assert (!FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m1.dmschema"));
    carbon.sql("drop model if exists db.m2");
    // create model with options
    carbon.sql(
        "CREATE MODEL if not exists db.m2 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/m2.dmschema"));
    carbon.sql("drop model if exists db.m2");
    carbon.sql("drop table if exists db.test");
  }

  /**
   * test query object
   */
  @Test
  public void testQueryObject()
      throws IOException, NoSuchDataMapException {
    carbon.sql("drop table if exists db.test");
    carbon.sql("create table db.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop model if exists db.m1");
    carbon.sql(
        "CREATE MODEL if not exists db.m1 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3=5");
    DataMapSchema m1 = ModelStoreManager.getInstance().getModelSchema("m1");
    assert(m1.getDataMapName().equalsIgnoreCase("m1"));
    assert(m1.getCtasQuery().equalsIgnoreCase(" select c1,c2 from db.test where c3=5"));
    // get Query Object
    String query = m1.getProperties().get(CarbonCommonConstants.QUERY_OBJECT);
    DataScan queryObject = (DataScan) ObjectSerializationUtil.convertStringToObject(query);
    String[] projects = new String[]{"c1", "c2"};
    // compare projection columns
    assert (Arrays.equals(queryObject.getProjectionColumns(), projects));
    // compare table name
    assert (queryObject.getTableName().equalsIgnoreCase("db_test"));
    // compare filter expression
    ColumnExpression columnExpression = new ColumnExpression("c3", DataTypes.INT);
    EqualToExpression equalToExpression = new EqualToExpression(columnExpression,
        new LiteralExpression("5", DataTypes.INT));
    assert (queryObject.getFilterExpression().getString().equals(equalToExpression.getString()));
  }

  @Test
  public void testModelWithModelArts() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    carbon.sql("drop database if exists a1 cascade");
    carbon.sql("create database a1");
    carbon.sql("drop table if exists a1.test");
    carbon.sql("create table a1.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop model if exists a1.m2");
    // create model with options
    carbon.sql(
        "CREATE MODEL if not exists a1.m2 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/_default_projectid_a1_m2.dmschema"));

    carbon.sql("start job job1 on model a1.m2 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();

    carbon.sql("drop model if exists a1.m2");
    carbon.sql("drop table if exists a1.test");
    carbon.sql("drop database if exists a1 cascade");
  }

  @AfterClass public static void tearDown() {
    carbon.sql("drop database if exists db cascade");
    carbon.close();
  }
}