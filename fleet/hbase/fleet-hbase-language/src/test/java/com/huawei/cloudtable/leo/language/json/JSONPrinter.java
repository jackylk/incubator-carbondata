package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.SyntaxTree;

import java.io.PrintStream;

public class JSONPrinter extends JSONElementVisitor {

  public JSONPrinter(final String lineBreak, final String lineRetraction) {
    this.lineBreak = lineBreak;
    this.lineRetraction = lineRetraction;
    this.stringBuilder = new StringBuilder();
    this.retractionCount = 0;
  }

  private final String lineBreak;

  private final String lineRetraction;

  private final StringBuilder stringBuilder;

  private int retractionCount;

  public String print(final JSONElement element) {
    this.reset();
    element.accept(this);
    return this.stringBuilder.toString();
  }

  public void print(final JSONElement element, final PrintStream stream) {
    this.reset();
    element.accept(this);
    stream.print(this.stringBuilder.toString());
  }

  @Override
  public void visit(final JSONConstants.String string) {
    string.toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONConstants.Integer integer) {
    integer.toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONConstants.Real real) {
    real.toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONConstants.Boolean bool) {
    bool.toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONNull none) {
    none.toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONArray array) {
    array.getLeftSquareBracket().toString(this.stringBuilder);
    if (array.getElements() != null) {
      this.retractionCount++;
      try {
        this.stringBuilder.append(this.lineBreak);
        this.printLineRetraction();
        final SyntaxTree.NodeList<JSONElement, JSONSymbols.COMMA> elements = array.getElements();
        for (int index = 0; index < elements.size(); index++) {
          if (index != 0) {
            elements.getDelimiter().toString(this.stringBuilder);
            this.stringBuilder.append(this.lineBreak);
            this.printLineRetraction();
          }
          elements.get(index).accept(this);
        }
      } finally {
        this.retractionCount--;
      }
      this.stringBuilder.append(this.lineBreak);
      this.printLineRetraction();
    }
    array.getRightSquareBracket().toString(this.stringBuilder);
  }

  @Override
  public void visit(final JSONObject object) {
    object.getLeftBrace().toString(this.stringBuilder);
    if (object.getFields() != null) {
      this.retractionCount++;
      try {
        this.stringBuilder.append(this.lineBreak);
        this.printLineRetraction();
        final SyntaxTree.NodeList<JSONObjectField, JSONSymbols.COMMA> fields = object.getFields();
        for (int index = 0; index < fields.size(); index++) {
          if (index != 0) {
            fields.getDelimiter().toString(this.stringBuilder);
            this.stringBuilder.append(this.lineBreak);
            this.printLineRetraction();
          }
          this.printObjectField(fields.get(index));
        }
      } finally {
        this.retractionCount--;
      }
      this.stringBuilder.append(this.lineBreak);
      this.printLineRetraction();
    }
    object.getRightBrace().toString(this.stringBuilder);
  }

  private void printLineRetraction() {
    for (int index = 0; index < this.retractionCount; index++) {
      this.stringBuilder.append(this.lineRetraction);
    }
  }

  private void printObjectField(final JSONObjectField objectField) {
    objectField.getName().toString(this.stringBuilder);
    objectField.getColon().toString(this.stringBuilder);
    this.stringBuilder.append(' ');
    objectField.getValue().accept(this);
  }

  private void reset() {
    this.stringBuilder.setLength(0);
    this.retractionCount = 0;
  }

}
