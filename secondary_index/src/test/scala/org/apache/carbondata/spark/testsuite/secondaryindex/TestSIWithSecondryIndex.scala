package org.apache.carbondata.spark.testsuite.secondaryindex

import org.apache.spark.sql.Row
import org.apache.spark.sql.common.util.QueryTest
import org.scalatest.BeforeAndAfterAll

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.CarbonProperties

/**
 * Created by K00900841 on 2017/9/1.
 */
class TestSIWithSecondryIndex extends QueryTest with BeforeAndAfterAll {

  override def beforeAll {
    sql("drop index if exists si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    CarbonProperties.getInstance()
      .addProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
        CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
      .addProperty(CarbonCommonConstants.ENABLE_UNSAFE_COLUMN_PAGE, "true")
    sql("create table table_WithSIAndAlter(c1 string, c2 date,c3 timestamp) stored by 'carbondata'")
    sql("insert into table_WithSIAndAlter select 'xx',current_date, current_timestamp")
    sql("alter table table_WithSIAndAlter add columns(date1 date, time timestamp)")
    sql("update table_WithSIAndAlter set(date1) = (c2)").show
    sql("update table_WithSIAndAlter set(time) = (c3)").show
    sql("create index si_altercolumn on table table_WithSIAndAlter(date1,time) as 'carbondata'")
  }

  test("Test secondry index data count") {
    checkAnswer(sql("select count(*) from si_altercolumn")
      ,Seq(Row(1)))
  }

  test("test create secondary index when all records are deleted from table") {
    sql("drop table if exists delete_records")
    sql("create table delete_records (a string,b string) stored by 'carbondata'")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("insert into delete_records values('k','r')")
    sql("delete from delete_records where a='k'").show()
    sql("alter table delete_records compact 'minor'")
    sql("create index index1 on table delete_records(b) as 'carbondata'")
    checkAnswer(sql("select count(*) from index1"), Row(0))
    sql("drop table if exists delete_records")
  }

  test("test secondary index data after parent table rename") {
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
    sql("create table maintable (a string,b string, c int) stored by 'carbondata'")
    sql("insert into maintable values('k','x',2)")
    sql("insert into maintable values('k','r',1)")
    sql("create index index21 on table maintable(b) as 'carbondata'")
    checkAnswer(sql("select * from maintable where c>1"), Seq(Row("k","x",2)))
    sql("ALTER TABLE maintable RENAME TO maintableeee")
    checkAnswer(sql("select * from maintableeee where c>1"), Seq(Row("k","x",2)))
  }

  override def afterAll {
    sql("drop index si_altercolumn on table_WithSIAndAlter")
    sql("drop table if exists table_WithSIAndAlter")
    sql("drop table if exists maintable")
    sql("drop table if exists maintableeee")
  }

}
