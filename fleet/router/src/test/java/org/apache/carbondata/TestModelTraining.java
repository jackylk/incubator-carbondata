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

import com.huawei.cloud.modelarts.DataScan;
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

import org.apache.spark.sql.LeoDatabase;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.leo.LeoEnv;
import org.apache.spark.sql.leo.ExperimentStoreManager;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * unit test for leo model ddl commands
 */
public class TestModelTraining {

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
    carbon.sql("drop experiment if exists m1");
    // create model without options
    carbon.sql("CREATE EXPERIMENT m1  as select c1,c2 as a from db.test where c3>5 and c2=1");
    String expName = LeoDatabase.DEFAULT_PROJECTID() + CarbonCommonConstants.UNDERSCORE + "m1";
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/" + expName + ".dmschema"));
    carbon.sql("drop experiment if exists m1");
    assert (!FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/" + expName + ".dmschema"));
    carbon.sql("drop experiment if exists m2");
    // create model with options
    carbon.sql(
        "CREATE EXPERIMENT if not exists m2 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3>5");
    expName = LeoDatabase.DEFAULT_PROJECTID() + CarbonCommonConstants.UNDERSCORE + "m2";
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/" + expName + ".dmschema"));
    carbon.sql("drop experiment if exists m2");
    carbon.sql("drop table if exists db.test");
  }

  /**
   * test query object
   */
  @Test
  public void testQueryObject()
      throws IOException, NoSuchDataMapException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    carbon.sql("drop table if exists db.test");
    carbon.sql("create table db.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop experiment if exists m1");
    carbon.sql(
        "CREATE experiment if not exists m1 OPTIONS('label_col'='c2', 'max_iteration'='100') "
            + "as select c1,c2 from db.test where c3=5");
    String expName = LeoDatabase.DEFAULT_PROJECTID() + CarbonCommonConstants.UNDERSCORE + "m1";
    DataMapSchema m1 = ExperimentStoreManager.getInstance().getExperimentSchema(expName);
    assert(m1.getDataMapName().equalsIgnoreCase(expName));
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
    carbon.sql("drop experiment if exists m2");
    // create model with options
    carbon.sql(
        "CREATE experiment if not exists m2 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    String expName = LeoDatabase.DEFAULT_PROJECTID() + CarbonCommonConstants.UNDERSCORE + "m2";
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/" + expName + ".dmschema"));

    carbon.sql("create model job1 using experiment m2 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();

    carbon.sql("drop model if exists job1 on experiment m2");

    carbon.sql("drop experiment if exists m2");
    carbon.sql("drop table if exists a1.test");
    carbon.sql("drop database if exists a1 cascade");
  }

  /**
   * test Model Metrics
   */
  @Test
  public void testExperimentMetrics() {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    carbon.sql("drop table if exists db.test");
    carbon.sql("create table db.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop experiment if exists e11");
    carbon.sql(
        "CREATE experiment if not exists e11 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from db.test where c3>5");
    carbon.sql("create model job1 using experiment e11 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();
    // get experiment info for experiment m
    carbon.sql("select * from experiment_info(e11)").show(false);
    carbon.sql("drop model if exists job1 on experiment e11").show(false);
    carbon.sql("drop experiment if exists e11");
    carbon.sql("drop table if exists db.test");
  }

  /**
   * test Job Metrics
   */
  @Test
  public void testJobMetrics() {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    carbon.sql("drop table if exists db.test");
    carbon.sql("create table db.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop experiment if exists m1");
    carbon.sql(
        "CREATE experiment if not exists m1 OPTIONS('worker_server_num'='1', "
            + "'app_url'='/obs-5b79/train_mnist/', 'boot_file_url'='/obs-5b79/train_mnist/train_mnist.py', "
            + "'data_url'='/obs-5b79/dataset-mnist/','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from db.test where c3>5");
    carbon.sql("drop model if exists j on experiment m1").show(false);
    carbon.sql("create model j using experiment m1 OPTIONS('train_url'='/obs-5b79/mnist-model/','params'='num_epochs=1')").show();
    // get job metrics for job j
    carbon.sql("select * from training_info(m1.j)").show(false);
    carbon.sql("drop model if exists j on experiment m1").show(false);
    carbon.sql("drop experiment if exists m1");
    carbon.sql("drop table if exists db.test");
  }

  @Test
  public void testModelWithModelArtsFlower() throws IOException {
    CarbonProperties.getInstance().addProperty("leo.ma.username", "hwstaff_l00215684");
    CarbonProperties.getInstance().addProperty("leo.ma.password", "@Huawei123");
    carbon.sql("drop database if exists a1 cascade");
    carbon.sql("create database a1");
    carbon.sql("drop table if exists a1.test");
    carbon.sql("create table a1.test(c1 int, c2 int, c3 int)");
    carbon.sql("drop experiment if exists flower_exp");
    // create model with options
    carbon.sql(
        "CREATE experiment if not exists flower_exp OPTIONS('worker_server_num'='1', "
            + "'model_id'='7', 'dataset_id'='7EkkgKp0hbbZH6wp3MU', 'dataset_name'='flower', 'dataset_version_name'='V002',"
            + "'dataset_version_id'='2liks7uf5BazuB4rWai','log_url'='/obs-5b79/train-log/','engine_id'='28','spec_id'='1') as select c1,c2 from a1.test where c3>5");
    assert (FileFactory.isFileExist(
        CarbonProperties.getInstance().getSystemFolderLocation() + "/model/flower_exp.dmschema"));
    carbon.sql("drop model if exists job1 on experiment flower_exp").show(false);

    carbon.sql("create model job1 using experiment flower_exp OPTIONS('train_url'='/obs-5b79/flower_model1/')").show();

    carbon.sql("select * from training_info(flower_exp.job1)").show(false);

    carbon.sql("REGISTER MODEL flower_exp.job1 AS flower_mod");
    carbon.sql("select * from model_info(flower_mod)").show(false);
    carbon.sql("drop table if exists a1.testIMG");
    carbon.sql("create table a1.testIMG(c1 string, c2 binary)");
    carbon.sql("LOAD DATA LOCAL INPATH '/home/root1/carbon/carbondata/fleet/router/src/test/resources/img.csv' INTO TABLE a1.testIMG OPTIONS('header'='false','DELIMITER'=',','binary_decoder'='baSe64')").show();
    carbon.sql("select flower_mod(c2) from a1.testIMG").show(false);
    carbon.sql("drop table if exists a1.testIMG");
    carbon.sql("drop model if exists job1 on experiment flower_exp");
    carbon.sql("drop experiment if exists flower_exp");
    carbon.sql("drop table if exists a1.test");
    carbon.sql("drop database if exists a1 cascade");
  }



  @AfterClass public static void tearDown() throws IOException {
//    carbon.sql("drop database if exists db cascade");
    carbon.close();
//    FileUtils.deleteDirectory(new File("./warehouse"));
  }
}