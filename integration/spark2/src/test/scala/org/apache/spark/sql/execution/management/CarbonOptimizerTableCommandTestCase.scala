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

package org.apache.spark.sql.execution.management

import org.apache.spark.sql.execution.command.management.CarbonOptimizeTableCommand
import org.apache.spark.sql.test.util.QueryTest
import org.scalatest.BeforeAndAfterAll

class CarbonOptimizerTableCommandTestCase extends QueryTest with BeforeAndAfterAll {

  var t1: Long = _
  var t2: Long = _
  var t3: Long = _

  override def beforeAll(): Unit = {
    sql("drop table if exists LINEITEM")
    sql("create table if not exists LINEITEM(L_ORDERKEY INT , L_PARTKEY INT , L_SUPPKEY string, L_LINENUMBER int, L_QUANTITY double, L_EXTENDEDPRICE double, L_DISCOUNT double, L_TAX double, L_RETURNFLAG string, L_LINESTATUS string, L_SHIPDATE date, L_COMMITDATE date, L_RECEIPTDATE date, L_SHIPINSTRUCT string, L_SHIPMODE string, L_COMMENT string) STORED AS carbondata TBLPROPERTIES('table_blocklet_size'='32', 'table_blocksize'='128', 'SORT_COLUMNS'='L_SHIPDATE, L_RECEIPTDATE')")
    val l1 = System.currentTimeMillis()
    sql("load data inpath '/opt/bigdata/spark-2.2.1-bin-hadoop2.7/spark-warehouse/tpchhive.db/lineitem/lineitem.tbl' into table LINEITEM options('dateformat'='yyyy-MM-dd', 'header'='false', 'bad_records_action'='force', 'delimiter'='|')")
    val l2 = System.currentTimeMillis()
    t1 = l2 - l1

    sql("drop table if exists LINEITEM_optimized")
    sql("create table if not exists LINEITEM_optimized(L_ORDERKEY INT , L_PARTKEY INT , L_SUPPKEY string, L_LINENUMBER int, L_QUANTITY double, L_EXTENDEDPRICE double, L_DISCOUNT double, L_TAX double, L_RETURNFLAG string, L_LINESTATUS string, L_SHIPDATE date, L_COMMITDATE date, L_RECEIPTDATE date, L_SHIPINSTRUCT string, L_SHIPMODE string, L_COMMENT string) STORED AS carbondata TBLPROPERTIES('table_blocklet_size'='32', 'table_blocksize'='128', 'SORT_COLUMNS'='L_SHIPDATE, L_RECEIPTDATE')")
    val l3 = System.currentTimeMillis()
    sql("load data inpath '/opt/bigdata/spark-2.2.1-bin-hadoop2.7/spark-warehouse/tpchhive.db/lineitem/lineitem.tbl' into table LINEITEM_optimized options('dateformat'='yyyy-MM-dd', 'header'='false', 'bad_records_action'='force', 'delimiter'='|')")
    val l4 = System.currentTimeMillis()
    t2 = l4 - l3
  }

  test("repartition table segment") {
    val l5 = System.currentTimeMillis()
    sql("optimize table LINEITEM_optimized options('segment'='0', 'partition_column'='L_SHIPDATE')")
    val l6 = System.currentTimeMillis()
    t3 = l6 - l5
  }

  test("query performance") {
    val t11 = System.currentTimeMillis()
    sql("select L_SHIPMODE, count(*) from LINEITEM group by L_SHIPMODE").show(100, false)
    val t12 = System.currentTimeMillis()
    sql("select L_SHIPMODE, count(*) from LINEITEM_optimized group by L_SHIPMODE").show(100, false)
    val t13 = System.currentTimeMillis()
    System.out.println("LINEITEM " + (t12 - t11))
    System.out.println("LINEITEM_optimized " + (t13 - t12))

    System.out.println("load time : " + t1 + " " + t2 + " " + t3)
  }

  override def afterAll: Unit = {

  }
}
