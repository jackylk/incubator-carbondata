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
package org.apache.carbondata.core.scan.executor;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.scan.executor.impl.DetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.MVCCVectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.executor.impl.VectorDetailQueryExecutor;
import org.apache.carbondata.core.scan.model.QueryModel;

import org.apache.hadoop.conf.Configuration;

/**
 * Factory class to get the query executor from RDD
 * This will return the executor based on query type
 */
public class QueryExecutorFactory {

  public static QueryExecutor getQueryExecutor(QueryModel queryModel, Configuration configuration) {
    if (queryModel.isVectorReader()) {
      boolean directV3Read = false;
      if (queryModel.getTableBlockInfos().size() == 1 && queryModel.getUpdateTimeStamp() == 0) {
        directV3Read =
            queryModel.getTableBlockInfos().get(0).getVersion() == ColumnarFormatVersion.V3;
      }
      if (queryModel.getTable().getTableInfo().getFactTable().getTableProperties()
          .get(CarbonCommonConstants.PRIMARY_KEY_COLUMNS) != null && !directV3Read) {
        return new MVCCVectorDetailQueryExecutor(configuration,
            queryModel.getUpdateTimeStamp() > 0);
      } else {
        return new VectorDetailQueryExecutor(configuration);
      }
//      return new VectorDetailQueryExecutor(configuration);
    } else {
      return new DetailQueryExecutor(configuration);
    }
  }
}
