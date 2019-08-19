package org.apache.carbondata;

import org.apache.carbondata.cloud.SparkS3Constants;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.leo.LeoEnv;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * Unit test for simple App.
 */
public class TestLeoPKTableDDL {
  /**
   * Rigorous Test :-)
   */
  @Test public void shouldAnswerWithTrue() {
    assertTrue(true);
  }

  @Test public void sqlTest() {

    SparkSession.Builder builder =
        SparkSession.builder().master("local").appName("JavaCarbonSessionExample")
            .config("spark.driver.host", "localhost")
            .config(SparkS3Constants.END_POINT, "obs.cn-north-7.ulanqab.huawei.com:5443")
            .config(SparkS3Constants.AK, "Q0T7MOHUY0KVLKMEEM3M")
            .config(SparkS3Constants.SK, "HWmgXbcIWu2333e2HVdjq19oD9AfJULAuEdRgyDO")
            //https://issues.apache.org/jira/browse/HIVE-16346
        .config("hive.warehouse.subdir.inherit.perms", false)
        .config("spark.sql.warehouse.dir", "s3a://leo/")
        .config("hive.exec.scratchdir", "s3a://leo/tmp/hive");

    SparkSession session = LeoEnv.getOrCreateLeoSession(builder);

    runSQL(session, "create database if not exists leodb ");
    runSQL(session, "create table if not exists leodb.test(id int, name string)"
        + " tblproperties('primary_key'='id')");
    runSQL(session, "drop table if exists leodb.test");
    runSQL(session, "drop database if exists leodb");

  }

  private void runSQL(SparkSession session, String s) {
    System.out.println(session.sql(s).queryExecution());
  }
}
