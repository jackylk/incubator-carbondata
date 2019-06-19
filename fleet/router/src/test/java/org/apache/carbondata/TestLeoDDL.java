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

import java.util.List;
import java.util.function.Supplier;

import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.LeoDatabase;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalog.Database;
import org.apache.spark.sql.leo.LeoEnv;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

/**
 * Unit test for testing leo DDL commands
 */
public class TestLeoDDL {

  private static SparkSession session;

  @BeforeClass
  public static void setup() {
    SparkSession.Builder builder = SparkSession.builder()
        .master("local")
        .config("spark.driver.host", "localhost");
    session = LeoEnv.getOrCreateLeoSession(builder);
    session.sparkContext().setLogLevel("ERROR");

    session.sql("drop table if exists db1.t1");
    session.sql("drop database if exists db1 cascade");
  }

  private void shouldFail(Supplier<Dataset<Row>> func, Class exceptionClazz,
      String expectedExceptionMessage) {
    try {
      func.get();
      System.out.println("expected " + exceptionClazz + ", but no exception is thrown");
      fail();
    } catch (Exception e) {
      if (!(e.getClass().equals(exceptionClazz))) {
        fail();
      } else {
        if (e.getMessage().contains(LeoDatabase.DEFAULT_PROJECTID())) {
          System.out.println("got invalid exception message \"\"\"" + expectedExceptionMessage);
          fail();
        } if (!e.getMessage().contains(expectedExceptionMessage)) {
          System.out.println("expected \"\"\"" + expectedExceptionMessage + "\"\"\", but "
              + "got \"\"\"" + e.getMessage() + "\"\"\"");
          fail();
        } else {
          System.out.println("Got an expected exception: " + e);
        }
      }
    }
  }

  /**
   * Test create table without giving database name
   */
  @Test
  public void testCreateTableWithoutDBName() {
    shouldFail(() -> session.sql("create table t1 (name string, age int)"),
        AnalysisException.class,
        "database name must be specified");
  }

  /**
   * Test create and drop database
   */
  @Test
  public void testCreateDBOk() throws AnalysisException {
    session.sql("create database db1");

    Database db = session.catalog().getDatabase(LeoDatabase.fromUserDBName("db1"));
    assertEquals(LeoDatabase.fromUserDBName("db1"), db.name());

    List<Row> rows = session.sql("show databases").collectAsList();
    assertEquals("db1", rows.get(0).getString(0));

    session.sql("drop database db1");
    rows = session.sql("show databases").collectAsList();
    assertEquals(0, rows.size());
  }

  /**
   * Can not create database name "default"
   */
  @Test
  public void testCreateDefaultDBFail() {
    shouldFail(() -> session.sql("create database default"),
        AnalysisException.class,
        "database name default is not allowed");
  }

  /**
   * Test create, drop table, show table, desc table, explain
   */
  @Test
  public void testCreateTableOk() {
    session.sql("create database db1");
    session.sql("create table db1.t1 (name string, age int)");

    List<Row> rows = session.sql("show tables in db1").collectAsList();
    assertEquals("db1", rows.get(0).getString(0));
    assertEquals("t1", rows.get(0).getString(1));
    assertFalse(rows.get(0).getBoolean(2));

    session.sql("desc formatted db1.t1").show(100, false);
    rows = session.sql("desc formatted db1.t1").collectAsList();
    long size = rows.stream()
        .filter(row -> row.getString(1).contains(LeoDatabase.getLeoDBPrefix()))
        .count();
    assertEquals(0, size);

    session.sql("explain select * from db1.t1").show(false);
    rows = session.sql("explain select * from db1.t1").collectAsList();
    size = rows.stream()
        .filter(row -> row.getString(0).contains(LeoDatabase.getLeoDBPrefix()))
        .count();
    assertEquals(0, size);

    rows = session.sql("select * from db1.t1").collectAsList();
    assertEquals(0, rows.size());

    session.sql("drop table db1.t1");
    session.sql("drop database db1");
  }

  /**
   * Test show table fail
   */
  @Test
  public void testShowTablesFail() {
    session.sql("create database db1");
    session.sql("create table db1.t1 (name string, age int)");

    shouldFail(() -> session.sql("show tables"),
        AnalysisException.class, "database name must be specified");

    shouldFail(() -> session.sql("show tables in default"),
        AnalysisException.class, "default database is not allowed");

    session.sql("drop table db1.t1");
    session.sql("drop database db1");
  }

  /**
   * Test desc table fail
   */
  @Test
  public void testDescTableFail() {
    session.sql("create database db1");
    session.sql("create table db1.t1 (name string, age int)");

    shouldFail(() -> session.sql("desc table t1"),
        AnalysisException.class, "database name must be specified");

    shouldFail(() -> session.sql("desc table default.t1"),
        AnalysisException.class, "default database is not allowed");

    session.sql("drop table db1.t1");
    session.sql("drop database db1");
  }

  /**
   * Test explain query fail
   */
  @Test
  public void testExplainFail() {
    session.sql("create database db1");
    session.sql("create table db1.t1 (name string, age int)");

    shouldFail(() -> session.sql("explain select * from t1"),
        AnalysisException.class, "database name must be specified");

    shouldFail(() -> session.sql("explain select * from default.t1"),
        AnalysisException.class, "default database is not allowed");

    session.sql("drop table db1.t1");
    session.sql("drop database db1");
  }

  /**
   * Test query fail
   */
  @Test
  public void testQueryFail() {
    session.sql("create database db1");
    session.sql("create table db1.t1 (name string, age int)");

    shouldFail(() -> session.sql("select * from t1"),
        AnalysisException.class, "database name must be specified");

    shouldFail(() -> session.sql("select * from default.t1"),
        AnalysisException.class, "default database is not allowed");

    shouldFail(() -> session.sql("select count(*) from (select * from t1)"),
        AnalysisException.class, "database name must be specified");

    session.sql("drop table db1.t1");
    session.sql("drop database db1");
  }
}
