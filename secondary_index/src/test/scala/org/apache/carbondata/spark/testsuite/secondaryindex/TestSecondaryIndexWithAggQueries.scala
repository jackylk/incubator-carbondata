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
 * test cases with secondary index and agg queries
 */
class TestSecondaryIndexWithAggQueries extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop table if exists source")
  }

  test("test agg queries with secondary index") {
    sql("create table source (c1 string,c2 string,c3 string,c5 string) STORED BY 'org.apache" +
        ".carbondata.format'")
    sql(s"""LOAD DATA LOCAL INPATH '$pluginResourcesPath/secindex/dest.csv' INTO table source""")
   /* sql("create index index_source1 on table source (c2) AS 'org.apache.carbondata.format'")
    checkAnswer(
      sql("select count(*) from source where c2='1' and c3 = 'aa' and c5 = 'aaa' "),
      Seq(Row(1))
    )
   sql("create index index_source2 on table source (c3) AS 'org.apache.carbondata.format'")

    checkAnswer(
      sql("select count(*) from source where c2='zc' and c3 = 'gf' and c5 = 'fd' "),
      Seq(Row(0))
    )
       sql("create index index_source3 on table source (c5) AS 'org.apache.carbondata.format'")
      checkAnswer(
        sql("select count(*) from source where c2='2' and c3 = 'bb' and c5 = 'bbb' "),
        Seq(Row(1))
      )*/
  }

  override def afterAll: Unit = {
    sql("drop table if exists source")
  }

}

