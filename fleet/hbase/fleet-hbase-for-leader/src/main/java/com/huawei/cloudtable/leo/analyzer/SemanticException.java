package com.huawei.cloudtable.leo.analyzer;

import com.huawei.cloudtable.leo.language.SyntaxTree;
import javax.annotation.Nullable;

public class SemanticException extends Exception {

  private static final long serialVersionUID = -8409759845240641535L;

  public SemanticException(final Reason reason, final SyntaxTree.Node node) {
    this(reason, node, null, null);
  }

  public SemanticException(final Reason reason, final SyntaxTree.Node node, final String message) {
    this(reason, node, message, null);
  }

  public SemanticException(final Reason reason, final SyntaxTree.Node node, final Throwable cause) {
    this(reason, node, null, cause);
  }

  public SemanticException(final Reason reason, final SyntaxTree.Node node, final String message, final Throwable cause) {
    super(message, cause);
    if (reason == null) {
      throw new IllegalArgumentException("Argument [reason] is null.");
    }
    this.reason = reason;
    this.nodePosition = node == null ? null : node.getPosition();
  }

  private final Reason reason;

  private final SyntaxTree.NodePosition nodePosition;

  public Reason getReason() {
    return this.reason;
  }

  @Nullable
  public SyntaxTree.NodePosition getNodePosition() {
    return this.nodePosition;
  }

  public enum Reason {

    TABLE_NOT_FOUND((short) 0),

    COLUMN_NOT_FOUND((short) 1),

    COLUMN_AMBIGUOUS((short) 2),

    COLUMN_COUNT_DIFFERENT((short) 3),

    COLUMN_INCOMPATIBILITY((short) 4),

    ALIAS_NOT_UNIQUE((short) 5),

    FUNCTION_NOT_FOUND((short) 6),

    FUNCTION_AMBIGUOUS((short) 7),

    UNSUPPORTED_GROUP_TYPE((short) 8),

    ILLEGAL_USE_OF_REFERENCE((short) 9),

    ILLEGAL_USE_OF_AGGREGATION((short) 10);

    Reason(final short code) {
      if (code < 0) {
        throw new IllegalArgumentException("Argument [code] is less than 0.");
      }
      this.code = code;
    }

    private final short code;

    public short getCode() {
      return this.code;
    }
  }

}
