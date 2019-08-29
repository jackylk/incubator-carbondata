package com.huawei.cloudtable.leo.language.json;

public abstract class JSONElementVisitor {

  public abstract void visit(JSONConstants.String string);

  public abstract void visit(JSONConstants.Integer integer);

  public abstract void visit(JSONConstants.Real real);

  public abstract void visit(JSONConstants.Boolean bool);

  public abstract void visit(JSONNull none);

  public abstract void visit(JSONArray array);

  public abstract void visit(JSONObject object);

}
