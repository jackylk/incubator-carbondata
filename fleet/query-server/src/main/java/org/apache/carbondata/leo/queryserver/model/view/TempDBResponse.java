package org.apache.carbondata.leo.queryserver.model.view;

import com.fasterxml.jackson.annotation.JsonProperty;

public class TempDBResponse extends Response {
  @JsonProperty("db_name")
  private String tempDBName;

  @JsonProperty("action")
  private String action;

  public TempDBResponse(TempDBRequest request) {
    this.tempDBName = request.getTempDBName();
    this.action = request.getAction();
  }

  public String getTempDBName() {
    return tempDBName;
  }

  public void setTempDBName(String tempDBName) {
    this.tempDBName = tempDBName;
  }

  public String getAction() {
    return action;
  }

  public void setAction(String action) {
    this.action = action;
  }

}
