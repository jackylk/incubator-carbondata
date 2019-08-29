package com.huawei.cloudtable.leo.language.statement;

import com.huawei.cloudtable.leo.language.SQLKeywords;
import com.huawei.cloudtable.leo.language.SyntaxTree;
import com.huawei.cloudtable.leo.language.annotation.Required;

public abstract class SQLGroupType extends SyntaxTree.Node {

  private SQLGroupType() {
    // to do nothing.
  }

  public static final class ROLLUP extends SQLGroupType {

    public ROLLUP(@Required final SQLKeywords.ROLLUP rollup) {
      if (rollup == null) {
        throw new IllegalArgumentException("Argument [rollup] is null.");
      }
      this.rollup = rollup;
    }

    private final SQLKeywords.ROLLUP rollup;

    @Override
    public String toString() {
      return this.rollup.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.rollup.toString(stringBuilder);
    }

  }

  public static final class CUBE extends SQLGroupType {

    public CUBE(@Required final SQLKeywords.CUBE cube) {
      if (cube == null) {
        throw new IllegalArgumentException("Argument [cube] is null.");
      }
      this.cube = cube;
    }

    private final SQLKeywords.CUBE cube;

    @Override
    public String toString() {
      return this.cube.toString();
    }

    @Override
    public void toString(final StringBuilder stringBuilder) {
      this.cube.toString(stringBuilder);
    }

  }

}
