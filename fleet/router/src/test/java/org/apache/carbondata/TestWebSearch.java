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

import org.junit.BeforeClass;
import org.junit.Test;

public class TestWebSearch extends LeoTest {

  @BeforeClass
  public static void setup() {
    sql("drop table if exists db1.t1");
    sql("drop database if exists db1 cascade");
    sql("create database db1");
  }

  @Test
  public void testUDTF() {
    sql("create table db1.t1 (name string, age int)");
    sql("select url, title from WebSearch('key_word'='特朗普', 'page_num'='3')").show(100, false);
    sql("drop table db1.t1");
  }

  @Test
  public void testDownload() {
    sql("create table db1.t1 (image binary)");
    sql("insert into db1.t1 select download('https://www.baidu.com/img/bd_logo1.png?qua=high&where=super')");
    sql("select * from db1.t1").show();
    sql("drop table db1.t1");
  }
}
