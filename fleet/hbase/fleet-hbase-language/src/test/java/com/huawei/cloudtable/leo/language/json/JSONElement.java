package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.SyntaxTree;

public abstract class JSONElement extends SyntaxTree.Node {

  public abstract void accept(JSONElementVisitor visitor);

}
