package com.huawei.cloudtable.leo.language;

public class SyntaxException extends Exception {

  private static final long serialVersionUID = -673964680276871326L;

  public SyntaxException(final SyntaxTree.NodePosition position) {
    super("Position: " + position.toString());
    this.position = position;
  }

  public SyntaxException(final SyntaxTree.NodePosition position, final String message) {
    super("Position: " + position.toString() + "   Message: " + message);
    this.position = position;
  }

  private final SyntaxTree.NodePosition position;

  public SyntaxTree.NodePosition getPosition() {
    return this.position;
  }

}
