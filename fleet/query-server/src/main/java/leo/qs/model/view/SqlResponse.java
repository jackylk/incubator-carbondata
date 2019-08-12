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

package leo.qs.model.view;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import leo.model.view.Schema;

public class SqlResponse extends Response {
  private String type;
  @JsonProperty("id")
  private String jobId;
  private Schema schema;
  @JsonProperty("rows_more")
  private boolean rowsMore = true;
  @JsonProperty("row_count")
  private Integer rowCount;
  private List<String[]> rows;
  private boolean async;

  public SqlResponse() {
  }

  public SqlResponse(Request request, String message,
                     String jobId, Schema schema, List<String[]> rows) {
    super(request, message);
    this.jobId = jobId;
    this.schema = schema;
    this.rows = rows;
  }

  public String getJobId() {
    return jobId;
  }

  public void setJobId(String jobId) {
    this.jobId = jobId;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Integer getRowCount() {
    return rowCount;
  }

  public void setRowCount(Integer rowCount) {
    this.rowCount = rowCount;
  }

  public List<String[]> getRows() {
    return rows;
  }

  public void setRows(List<String[]> rows) {
    this.rows = rows;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean isRowsMore() {
    return rowsMore;
  }

  public void setRowsMore(boolean rowsMore) {
    this.rowsMore = rowsMore;
  }

  public boolean isAsync() {
    return async;
  }

  public void setAsync(boolean async) {
    this.async = async;
  }
}
