/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

/**
 * test cases for IUD data retention on SI tables
 */
class TestSecondaryIndexWithIUD extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists dest")
    sql("drop table if exists source")
    sql("drop table if exists test")
  }

  /*test("test index with IUD delete all_rows") {

    sql(
      "create table dest (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata" +
      ".format'")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    sql("drop index if exists index_dest1 on dest")
    sql("create index index_dest1 on table dest (c3) AS 'org.apache.carbondata.format'")
    sql("drop index if exists index_dest2 on dest")
    //create second index table , result should be same
    sql("create index index_dest2 on table dest (c3,c5) AS 'org.apache.carbondata.format'")
    // delete all rows in the segment
    sql("delete from dest d where d.c2 not in (56)").show
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )
    sql("show segments for table index_dest1").show(false)
    assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
             .equals(CarbonCommonConstants.MARKED_FOR_DELETE))
    assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
             .equals(CarbonCommonConstants.MARKED_FOR_DELETE))

    // execute clean files
    sql("clean files for table dest")

    sql("show segments for table index_dest2").show()
    val exception_index_dest1 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest1").collect()(0).get(1).toString()
        .equals(CarbonCommonConstants.MARKED_FOR_DELETE))
    }
    val exception_index_dest2 = intercept[IndexOutOfBoundsException] {
      assert(sql("show segments for table index_dest2").collect()(0).get(1).toString()
        .equals(CarbonCommonConstants.MARKED_FOR_DELETE))
    }

    //load again and check result
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table dest""")
    checkAnswer(
      sql("""select c3 from dest"""),
      sql("""select c3 from index_dest1""")
    )
    checkAnswer(
      sql("""select c3,c5 from dest"""),
      sql("""select c3,c5 from index_dest2""")
    )


  }*/

  test("test index with IUD delete all_rows-1") {
    sql(
      "create table source (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache" +
      ".carbondata.format'")
    sql(s"""LOAD DATA LOCAL INPATH '$pluginResourcesPath/IUD/dest.csv' INTO table source""")
    sql("drop index if exists index_source1 on source")
    sql("create index index_source1 on table source (c5) AS 'org.apache.carbondata.format'")
    // delete (5-1)=4 rows
    try {
      sql("""delete from source d where d.c2 in (1,2,3,4)""").show
      assert(false)
    }
    catch {
      case ex: Exception => assert(true)
        // results should not be same
        val exception = intercept[Exception] {
          checkAnswer(
            sql("""select c5 from source"""),
            sql("""select c5 from index_source1""")
          )
        }
    }
    // crete second index table
    sql("drop index if exists index_source2 on source")
    sql("create index index_source2 on table source (c3) AS 'org.apache.carbondata.format'")
    // result should be same
      checkAnswer(
        sql("""select c3 from source"""),
        sql("""select c3 from index_source2""")
      )
    sql("clean files for table source")
    sql("show segments for table index_source2").show()
    /*assert(sql("show segments for table index_source2").collect()(0).get(1).toString()
      .equals(CarbonCommonConstants.STORE_LOADSTATUS_SUCCESS))*/
  }

/*  test("test index with IUD delete using Join") {
    sql(
      "create table test (c1 string,c2 int,c3 string,c5 string) STORED BY 'org.apache.carbondata" +
      ".format'")
    sql(s"""LOAD DATA LOCAL INPATH '$resourcesPath/IUD/dest.csv' INTO table test""")
    sql("drop index if exists index_test1 on test")
    sql("create index index_test1 on table test (c3) AS 'org.apache.carbondata.format'")
    // delete all rows in the segment
    sql("delete from test d where d.c2 not in (56)").show
    checkAnswer(
      sql(
        "select test.c3, index_test1.c3 from test right join index_test1  on test.c3 =  " +
        "index_test1.c3"),
      Seq())
  }*/

  test("test if secondary index gives correct result on limit query after row deletion") {
    sql("drop table if exists t10")
    sql("create table t10(id int, country string) stored by 'carbondata' TBLPROPERTIES" +
        "('DICTIONARY_INCLUDE'='id')").show()
    sql("create index si3 on table t10(country) as 'carbondata'")
    sql(
      s" load data INPATH '$pluginResourcesPath/IUD/sample_1.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    sql(
      s" load data INPATH '$pluginResourcesPath/IUD/sample_2.csv' INTO table t10 options " +
      "('DELIMITER'=',','FILEHEADER'='id,country')")
    try {
      sql("delete from t10 where id in (1,2)").show()
    assert(false)
    }
    catch {
      case ex: Exception => assert(true)
    }
    sql(" select *  from t10").show()
    checkAnswer(sql(" select country from t10 where country = 'china' order by id limit 1"), Row("china"))
  }

  override def afterAll: Unit = {
    sql("drop table if exists dest")
    sql("drop table if exists source")
    sql("drop table if exists test")
  }
}
