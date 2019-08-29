package com.huawei.cloudtable.leo.language.json;

import com.huawei.cloudtable.leo.language.Lexical;

public final class JSONSymbols {

  @Lexical.Symbol.Declare('\"')
  public static final class DOUBLE_QUOTA extends Lexical.Symbol {

    public DOUBLE_QUOTA() {

    }

  }

  @Lexical.Symbol.Declare(',')
  public static final class COMMA extends Lexical.Symbol {

    public COMMA() {

    }

  }

  @Lexical.Symbol.Declare('{')
  public static final class L_BRACE extends Lexical.Symbol {

    public L_BRACE() {

    }

  }

  @Lexical.Symbol.Declare('}')
  public static final class R_BRACE extends Lexical.Symbol {

    public R_BRACE() {

    }

  }

  @Lexical.Symbol.Declare('[')
  public static final class L_SQUARE_BRACKET extends Lexical.Symbol {

    public L_SQUARE_BRACKET() {

    }

  }

  @Lexical.Symbol.Declare(']')
  public static final class R_SQUARE_BRACKET extends Lexical.Symbol {

    public R_SQUARE_BRACKET() {

    }

  }

  @Lexical.Symbol.Declare(':')
  public static final class COLON extends Lexical.Symbol {

    public COLON() {

    }

  }

  @Lexical.Symbol.Declare('.')
  public static final class POINT extends Lexical.Symbol {

    public POINT() {

    }

  }

}
