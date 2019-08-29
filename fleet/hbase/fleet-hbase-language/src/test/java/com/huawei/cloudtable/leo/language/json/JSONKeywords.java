package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.Lexical;

public final class JSONKeywords {

  @Lexical.Keyword.Declare(value = "TRUE", caseSensitive = false)
  public static final class TRUE extends Lexical.Keyword {

    public TRUE() {

    }

  }

  @Lexical.Keyword.Declare(value = "FALSE", caseSensitive = false)
  public static final class FALSE extends Lexical.Keyword {

    public FALSE() {

    }

  }

  @Lexical.Keyword.Declare(value = "NULL", caseSensitive = false)
  public static final class NULL extends Lexical.Keyword {

    public NULL() {

    }

  }

}
