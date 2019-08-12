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

package leo.job;

import java.util.HashMap;
import java.util.Map;

public class QueryDef {

  private QueryType queryType;
  private static Map<String, QueryTypeDef> queryTypeDef = new HashMap<String, QueryTypeDef>();

  static {
    ///////////////////////////////////////////////////////////////
    //                            DDL                            //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.CREATE_DATABASE, true, true, false);
    addDef(QueryType.DROP_DATABASE, true, true, false);
    addDef(QueryType.CREATE_TABLE, true, true, false);
    addDef(QueryType.DROP_TABLE, true, true, false);
    addDef(QueryType.SET_DATABASE, true, true, false);
    addDef(QueryType.SHOW_DATABASES, true, true, false);
    addDef(QueryType.SHOW_TABLES, true, true, false);
    addDef(QueryType.DESC_TABLE, true, true, false);
    addDef(QueryType.DESC_COLUMN, true, true, false);
    addDef(QueryType.EXPLAIN, true, true, false);
    addDef(QueryType.ALTER_TABLE_ADD_COLUMN, true, false, false);
    addDef(QueryType.ALTER_TABLE_DROP_COLUMN, true, false, false);
    addDef(QueryType.ALTER_TABLE_INSERT_COLUMN, true, false, false);
    addDef(QueryType.ALTER_TABLE_ADD_PARTITION, true, false, false);
    addDef(QueryType.ALTER_TABLE_DROP_PARTITION, true, false, false);
    addDef(QueryType.ALTER_TABLE_RENAME_DATATYPE, true, false, false);
    addDef(QueryType.ALTER_TABLE_RENAME, true, false, false);
    addDef(QueryType.ALTER_TABLE_SET, true, false, false);
    addDef(QueryType.ALTER_TABLE_UNSET, true, false, false);
    addDef(QueryType.ALTER_TABLE_SPLIT_PARTITION, true, false, false);
    addDef(QueryType.SHOW_LOADS, true, false, false);

    ///////////////////////////////////////////////////////////////
    //                            DML                            //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.LOAD, true, false, true);
    addDef(QueryType.CARBON_INSERT_SELECT, true, false, true);
    addDef(QueryType.HBASE_INSERT, false, true, false);
    addDef(QueryType.BULK_UPDATE, true, false, true);
    addDef(QueryType.BULK_DELETE, true, false, true);
    addDef(QueryType.COMPACTION, true, false, true);
    addDef(QueryType.HANDOFF_MVCC, true, false, true);
    addDef(QueryType.CLEAN_FILES, true, false, true);
    addDef(QueryType.DELETE_LOAD_BY_ID, true, false, true);
    addDef(QueryType.DELETE_LOAD_BY_DATE, true, false, true);

    ///////////////////////////////////////////////////////////////
    //                       Select Query                        //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.HBASE_SELECT, false, true, false);
    addDef(QueryType.CARBON_SELECT, true, false, true);

    ///////////////////////////////////////////////////////////////
    //                            Data Map                       //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.CREATE_DATA_MAP, true, false, true);
    addDef(QueryType.DROP_DATA_MAP, true, false, false);
    addDef(QueryType.SHOW_DATA_MAP, true, false, false);
    addDef(QueryType.REBUILD_DATA_MAP, true, false, true);

    ///////////////////////////////////////////////////////////////
    //                            Stream                         //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.CREATE_STREAM, true, false, false);
    addDef(QueryType.DROP_STREAM, true, false, false);
    addDef(QueryType.SHOW_STREAM, true, false, false);
    addDef(QueryType.FINISH_STREAM, true, false, false);

    ///////////////////////////////////////////////////////////////
    //                          AI Query                         //
    ///////////////////////////////////////////////////////////////
    // will insert data
    addDef(QueryType.INSERT_COLUMNS, true, false, true);
    addDef(QueryType.CREATE_MODEL, true, false, false);
    addDef(QueryType.DROP_MODEL, true, false, true);
    addDef(QueryType.SHOW_MODELS, true, false, true);
    addDef(QueryType.START_JOB, true, false, true);
    addDef(QueryType.STOP_JOB, true, false, true);
    addDef(QueryType.REGISTER_MODEL, true, false, true);
    addDef(QueryType.UNREGISTER_MODEL, true, false, true);
    addDef(QueryType.RUN_SCRIPT, true, false, true);

    ///////////////////////////////////////////////////////////////
    //                          Consumer                         //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.CREATE_CONSUMER, true, false, false);
    addDef(QueryType.DROP_CONSUMER, true, false, false);
    addDef(QueryType.DESC_CONSUMER, true, false, false);
    addDef(QueryType.SHOW_CONSUMERS, true, false, false);

    ///////////////////////////////////////////////////////////////
    //                          Others                           //
    ///////////////////////////////////////////////////////////////
    addDef(QueryType.SET_COMMAND, true, true, false);

  }

  private static void addDef(QueryType type,
      boolean allowInNPKTable, boolean allowInPKTable, boolean executeAsync) {
    queryTypeDef.put(type.name(),
        new QueryTypeDef(type, allowInNPKTable, allowInPKTable, executeAsync));
  }

  public enum QueryType {

    ///////////////////////////////////////////////////////////////
    //                            DDL                            //
    ///////////////////////////////////////////////////////////////
    CREATE_DATABASE,
    DROP_DATABASE,
    CREATE_TABLE,
    DROP_TABLE,
    SET_DATABASE,
    SHOW_DATABASES,
    SHOW_TABLES,
    SHOW_LOADS,
    DESC_TABLE,
    DESC_COLUMN,
    EXPLAIN,
    ALTER_TABLE_ADD_COLUMN,
    ALTER_TABLE_DROP_COLUMN,
    ALTER_TABLE_INSERT_COLUMN,
    ALTER_TABLE_ADD_PARTITION,
    ALTER_TABLE_DROP_PARTITION,
    ALTER_TABLE_RENAME_DATATYPE,
    ALTER_TABLE_RENAME,
    ALTER_TABLE_SET,
    ALTER_TABLE_UNSET,
    ALTER_TABLE_SPLIT_PARTITION,

    ///////////////////////////////////////////////////////////////
    //                            DML                            //
    ///////////////////////////////////////////////////////////////
    LOAD,
    CARBON_INSERT_SELECT,
    HBASE_INSERT,
    BULK_UPDATE,
    BULK_DELETE,
    COMPACTION,
    CLEAN_FILES,
    DELETE_LOAD_BY_ID,
    DELETE_LOAD_BY_DATE,
    HANDOFF_MVCC,

    ///////////////////////////////////////////////////////////////
    //                       Select Query                        //
    ///////////////////////////////////////////////////////////////
    HBASE_SELECT,
    CARBON_SELECT,

    ///////////////////////////////////////////////////////////////
    //                            Data Map                       //
    ///////////////////////////////////////////////////////////////
    CREATE_DATA_MAP,
    DROP_DATA_MAP,
    SHOW_DATA_MAP,
    REBUILD_DATA_MAP,

    ///////////////////////////////////////////////////////////////
    //                            Stream                         //
    ///////////////////////////////////////////////////////////////
    CREATE_STREAM,
    DROP_STREAM,
    SHOW_STREAM,
    FINISH_STREAM,

    ///////////////////////////////////////////////////////////////
    //                          AI Query                         //
    ///////////////////////////////////////////////////////////////
    INSERT_COLUMNS,
    CREATE_MODEL,
    DROP_MODEL,
    SHOW_MODELS,
    START_JOB,
    STOP_JOB,
    REGISTER_MODEL,
    UNREGISTER_MODEL,
    RUN_SCRIPT,

    ///////////////////////////////////////////////////////////////
    //                          Consumer                         //
    ///////////////////////////////////////////////////////////////
    CREATE_CONSUMER,
    DROP_CONSUMER,
    DESC_CONSUMER,
    SHOW_CONSUMERS,
    ///////////////////////////////////////////////////////////////
    //                          Others                           //
    ///////////////////////////////////////////////////////////////
    SET_COMMAND

  }

  public static class QueryTypeDef {
    QueryType type;
    // true if this query type is allowed in non primary key table
    boolean allowInNPKTable;
    // true if this query type is allowed in primary key table
    boolean allowInPKTable;
    // true if this query type is executed sync, else async.
    boolean executeAsync;

    public QueryType getType() {
      return type;
    }

    public boolean isAllowInNPKTable() {
      return allowInNPKTable;
    }

    public boolean isAllowInPKTable() {
      return allowInPKTable;
    }

    public boolean isExecuteAsync() {
      return executeAsync;
    }

    public QueryTypeDef(QueryType type, boolean allowInNPKTable, boolean allowInPKTable,
        boolean executeAsync) {
      this.type = type;
      this.allowInNPKTable = allowInNPKTable;
      this.allowInPKTable = allowInPKTable;
      this.executeAsync = executeAsync;


    }
  }

  public static QueryTypeDef getQueryTypeDef(String typeName) {
    return queryTypeDef.get(typeName);
  }

}
