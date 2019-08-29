package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.expression.*;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ExpressionCodec {

  public static byte[] encode(final EncodeContext context, final Expression<?> expression) {
    final Encoder encoder = ENCODER_CACHE.get();
    try {
      encoder.encode(context, expression);
      return encoder.getBytes();
    } finally {
      encoder.reset();
    }
  }

  public static void encode(final EncodeContext context, final Expression<?> expression, final ByteBuffer byteBuffer) {
    new Encoder(byteBuffer).encode(context, expression);
  }

  public static Expression decode(final DecodeContext context, final ByteBuffer byteBuffer) {
    return DECODER_CACHE.get().decode(context, byteBuffer);
  }

  static final byte TYPE_CODE_NULL = -1;

  static final byte TYPE_CODE_FUNCTION = 0;

  static final byte TYPE_CODE_CASTING = 1;

  static final byte TYPE_CODE_CONSTANT = 2;

  static final byte TYPE_CODE_REFERENCE = 3;

  static final byte TYPE_CODE_VARIABLE = 4;

  static final byte TYPE_CODE_LIKE = 5;

  static final byte TYPE_CODE_LOGICAL_AND = 6;

  static final byte TYPE_CODE_LOGICAL_XOR = 7;

  static final byte TYPE_CODE_LOGICAL_NOT = 8;

  static final byte TYPE_CODE_LOGICAL_OR = 9;

  static final byte TYPE_CODE_CASE = 10;

  static final byte VALUE_TRUE = 0;

  static final byte VALUE_FALSE = 1;

  private static final ThreadLocal<Encoder> ENCODER_CACHE = new ThreadLocal<Encoder>() {
    @Override
    protected Encoder initialValue() {
      return new Encoder();
    }
  };

  private static final ThreadLocal<Decoder> DECODER_CACHE = new ThreadLocal<Decoder>() {
    @Override
    protected Decoder initialValue() {
      return new Decoder();
    }
  };

  public interface EncodeContext {

    ValueCodecManager getValueCodecManager();

  }

  private static final class Encoder extends ExpressionVisitor<Void, EncodeContext> {

    Encoder() {
      this(ByteBuffer.allocate(10240));// TODO Get capacity from configuration.
    }

    Encoder(final ByteBuffer byteBuffer) {
      this.stringEncoder = Charset.forName("UTF-8").newEncoder(); // TODO Get charset from configuration.
      this.byteBuffer = byteBuffer;
    }

    private final CharsetEncoder stringEncoder;

    private final ByteBuffer byteBuffer;

    ByteBuffer getByteBuffer() {
      return this.byteBuffer;
    }

    void reset() {
      this.byteBuffer.clear();
    }

    byte[] getBytes(){
      final byte[] bytes = new byte[this.byteBuffer.position()];
      this.byteBuffer.position(0);
      this.byteBuffer.get(bytes);
      return bytes;
    }

    void encode(final EncodeContext context, final Expression<?> expression) {
      if (expression == null) {
        this.byteBuffer.put(TYPE_CODE_NULL);
      } else {
        expression.accept(this, context);
      }
    }

    private void encode(final Class<?> clazz) {
      this.encode(clazz.getName());
    }

    private void encode(final String string) {
      this.byteBuffer.position(this.byteBuffer.position() + 4);
      final int startPosition = this.byteBuffer.position();
      this.stringEncoder.encode(CharBuffer.wrap(string), this.byteBuffer, true);// TODO 此处的CharBuffer.wrap()会导致较大的性能损失(15倍)，后续可考虑直接将String类型改成CharBuffer
      final int byteCount = this.byteBuffer.position() - startPosition;
      this.byteBuffer.position(startPosition - 4);
      this.byteBuffer.putInt(byteCount);
      this.byteBuffer.position(startPosition + byteCount);
    }

    private void encode(final boolean aBoolean) {
      this.byteBuffer.put(aBoolean ? VALUE_TRUE : VALUE_FALSE);
    }

    @Override
    public Void visit(Case caze, EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_CASE);
      this.byteBuffer.putInt(caze.getBranches().size());
      List<Case.Branch<?>> branches = caze.getBranches();
      for (Case.Branch branch: branches) {
        branch.getCondition().accept(this, context);
        if (branch.getOperate() != null) {
          this.encode(true);
          branch.getOperate().accept(this, context);
        } else {
          this.encode(false);
        }
      }

      if (caze.getOperateElse() != null) {
        this.encode(true);
        caze.getOperateElse().accept(this, context);
      } else {
        this.encode(false);
      }
      return null;
    }

    @Override
    public Void visit(final Casting casting, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_CASTING);
      casting.getSource().accept(this, context);
      this.encode(casting.getConverter().getTargetClass());
      return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Void visit(final Constant constant, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_CONSTANT);
      final Object value = constant.getValue();
      final Class valueClass = value.getClass();
      this.encode(valueClass);
      final ValueCodec valueCodec = context.getValueCodecManager().getValueCodec(valueClass);
      if (valueCodec == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (valueCodec.isFixedLength()) {
        valueCodec.encode(value, this.byteBuffer);
      } else {
        valueCodec.encodeWithLength(value, this.byteBuffer);
      }
      if (constant.getValueString() != null) {
        this.encode(true);
        this.encode(constant.getValueString());
      } else {
        this.encode(false);
      }
      return null;
    }

    @Override
    public Void visit(final Reference reference, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_REFERENCE);// TODO 要考虑存储的是列名
      this.byteBuffer.putInt(reference.getAttributeIndex());
      this.encode(reference.getResultClass());
      this.encode(reference.isNullable());
      return null;
    }

    @Override
    public Void visit(final Variable variable, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_VARIABLE);
      this.byteBuffer.putInt(variable.getIndex());
      this.encode(variable.getResultClass());
      return null;
    }

    @Override
    public Void visit(final LikeExpression like, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_LIKE);
      // TODO
      throw new UnsupportedOperationException();
    }

    @Override
    public Void visit(final LogicalAnd logicalAnd, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_LOGICAL_AND);
      logicalAnd.getParameter1().accept(this, context);
      logicalAnd.getParameter2().accept(this, context);
      return null;
    }

    @Override
    public Void visit(final LogicalXor logicalXor, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_LOGICAL_XOR);
      logicalXor.getParameter1().accept(this, context);
      logicalXor.getParameter2().accept(this, context);
      return null;
    }

    @Override
    public Void visit(final LogicalNot logicalNot, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_LOGICAL_NOT);
      logicalNot.getParameter().accept(this, context);
      return null;
    }

    @Override
    public Void visit(final LogicalOr logicalOr, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_LOGICAL_OR);
      logicalOr.getParameter1().accept(this, context);
      logicalOr.getParameter2().accept(this, context);
      return null;
    }

    @Override
    public Void visit(final Function<?> function, final EncodeContext context) {
      this.byteBuffer.put(TYPE_CODE_FUNCTION);
      this.encode(function.getName().toString());
      this.byteBuffer.putInt(function.getParameterCount());
      for (int index = 0; index < function.getParameterCount(); index++) {
        this.encode(context, function.getParameter(index));
      }
      return null;
    }

  }

  public interface DecodeContext {

    ValueCodecManager getValueCodecManager();

    FunctionManager getFunctionManager();

  }

  private static final class Decoder {

    private static final int FUNCTION_PARAMETERS_TEMPLATE_CACHE_SIZE = 10;

    private static final ThreadLocal<Map<Integer, Evaluation<?>[]>> FUNCTION_PARAMETERS_TEMPLATE_CACHE = new ThreadLocal<Map<Integer, Evaluation<?>[]>>() {
      @Override
      protected Map<Integer, Evaluation<?>[]> initialValue() {
        return new HashMap<>();
      }
    };

    private static Evaluation<?>[] getFunctionParametersTemplate(final int parameterCount) {
      if (parameterCount > FUNCTION_PARAMETERS_TEMPLATE_CACHE_SIZE) {
        return new Evaluation<?>[parameterCount];
      } else {
        final Map<Integer, Evaluation<?>[]> cache = FUNCTION_PARAMETERS_TEMPLATE_CACHE.get();
        Evaluation<?>[] template = cache.get(parameterCount);
        if (template == null) {
          template = new Evaluation<?>[parameterCount];
          cache.put(parameterCount, template);
        }
        return template;
      }
    }

    Decoder() {
      this.stringDecoder = Charset.forName("UTF-8").newDecoder(); // TODO Get charset from configuration.
    }

    private final CharsetDecoder stringDecoder;

    Expression<?> decode(final DecodeContext context, final ByteBuffer byteBuffer) {
      final byte typeCode = byteBuffer.get();
      switch (typeCode) {
        case TYPE_CODE_NULL:
          return null;
        case TYPE_CODE_FUNCTION:
          return decodeFunction(context, byteBuffer);
        case TYPE_CODE_CASE:
          return this.decodeCase(context, byteBuffer);
        case TYPE_CODE_CASTING:
          return this.decodeCasting(context, byteBuffer);
        case TYPE_CODE_CONSTANT:
          return this.decodeConstant(context, byteBuffer);
        case TYPE_CODE_REFERENCE:
          return this.decodeReference(byteBuffer);
        case TYPE_CODE_LIKE:
          return this.decodeLike(context, byteBuffer);
        case TYPE_CODE_VARIABLE:
          return this.decodeVariable(byteBuffer);
        case TYPE_CODE_LOGICAL_AND:
          return this.decodeLogicalAnd(context, byteBuffer);
        case TYPE_CODE_LOGICAL_XOR:
          return this.decodeLogicalXor(context, byteBuffer);
        case TYPE_CODE_LOGICAL_NOT:
          return this.decodeLogicalNot(context, byteBuffer);
        case TYPE_CODE_LOGICAL_OR:
          return this.decodeLogicalOr(context, byteBuffer);
        default:
          throw new RuntimeException("Unsupported type code. " + typeCode);
      }
    }

    private Class<?> decodeClass(final ByteBuffer byteBuffer) {
      // TODO
      try {
        return Class.forName(decodeString(byteBuffer));
      } catch (ClassNotFoundException exception) {
        // TODO
        throw new UnsupportedOperationException(exception);
      }
    }

    private String decodeString(final ByteBuffer byteBuffer) {
      final int byteCount = byteBuffer.getInt();
      final int originalLimit = byteBuffer.limit();
      try {
        byteBuffer.limit(byteBuffer.position() + byteCount);
        try {
          return this.stringDecoder.decode(byteBuffer).toString();
        } catch (CharacterCodingException exception) {
          throw new RuntimeException(exception);
        }
      } finally {
        byteBuffer.limit(originalLimit);
      }
    }

    private boolean decodeBoolean(final ByteBuffer byteBuffer) {
      switch (byteBuffer.get()) {
        case VALUE_TRUE:
          return true;
        case VALUE_FALSE:
          return false;
        default:
          throw new RuntimeException();
      }
    }

    private Case decodeCase(final DecodeContext context, final ByteBuffer byteBuffer) {
      int brancheSize = byteBuffer.getInt();
      List<Case.Branch<?>> branches = new ArrayList<>(brancheSize);
      for (int i = 0; i < brancheSize; i++) {
        final Expression condition = this.decode(context, byteBuffer);
        if (!(condition instanceof Evaluation) || condition.getResultClass() != Boolean.class) {
          throw new UnsupportedOperationException();
        }

        final Expression operate;
        if (this.decodeBoolean(byteBuffer)) {
          operate = this.decode(context, byteBuffer);
          if (!(condition instanceof Evaluation)) {
            throw new UnsupportedOperationException();
          }
        } else {
          operate = null;
        }

        branches.add(new Case.Branch((Evaluation<Boolean>) condition, (Evaluation<?>) operate));
      }

      final Expression operateElze;
      if (this.decodeBoolean(byteBuffer)) {
        operateElze = this.decode(context, byteBuffer);
        if (!(operateElze instanceof Evaluation)) {
          throw new UnsupportedOperationException();
        }
      } else {
        operateElze = null;
      }

      return new Case(branches, (Evaluation<?>) operateElze);
    }

    @SuppressWarnings("unchecked")
    private Casting decodeCasting(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Expression source = this.decode(context, byteBuffer);
      if (!(source instanceof Evaluation)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      final Class targetClass = this.decodeClass(byteBuffer);
      final ValueConverter valueConverter = context.getFunctionManager().getConverter(source.getResultClass(), targetClass);
      if (valueConverter == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return new Casting((Evaluation) source, valueConverter);
    }

    @SuppressWarnings("unchecked")
    private Constant decodeConstant(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Object value;
      final Class valueClass = this.decodeClass(byteBuffer);
      final ValueCodec valueCodec = context.getValueCodecManager().getValueCodec(valueClass);
      if (valueCodec == null) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (valueCodec.isFixedLength()) {
        value = valueCodec.decode(byteBuffer);
      } else {
        value = valueCodec.decodeWithLength(byteBuffer);
      }
      if (this.decodeBoolean(byteBuffer)) {
        return new Constant(valueClass, value, this.decodeString(byteBuffer));
      } else {
        return new Constant(valueClass, value);
      }
    }

    @SuppressWarnings("unchecked")
    private Reference decodeReference(final ByteBuffer byteBuffer) {
      return new Reference(byteBuffer.getInt(), this.decodeClass(byteBuffer), this.decodeBoolean(byteBuffer));
    }

    private LikeExpression decodeLike(final DecodeContext context, final ByteBuffer byteBuffer) {
      // TODO
      throw new UnsupportedOperationException();
    }

    @SuppressWarnings("unchecked")
    private Variable decodeVariable(final ByteBuffer byteBuffer) {
      return new Variable(this.decodeClass(byteBuffer), byteBuffer.getInt());
    }

    @SuppressWarnings("unchecked")
    private LogicalAnd decodeLogicalAnd(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Expression<?> parameter1 = this.decode(context, byteBuffer);
      final Expression<?> parameter2 = this.decode(context, byteBuffer);
      return new LogicalAnd(asBooleanEvaluation(parameter1), asBooleanEvaluation(parameter2));
    }

    @SuppressWarnings("unchecked")
    private LogicalXor decodeLogicalXor(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Expression<?> parameter1 = this.decode(context, byteBuffer);
      final Expression<?> parameter2 = this.decode(context, byteBuffer);
      return new LogicalXor(asBooleanEvaluation(parameter1), asBooleanEvaluation(parameter2));
    }

    @SuppressWarnings("unchecked")
    private LogicalNot decodeLogicalNot(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Expression<?> parameter = this.decode(context, byteBuffer);
      return new LogicalNot(asBooleanEvaluation(parameter));
    }

    @SuppressWarnings("unchecked")
    private LogicalOr decodeLogicalOr(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Expression<?> parameter1 = this.decode(context, byteBuffer);
      final Expression<?> parameter2 = this.decode(context, byteBuffer);
      return new LogicalOr(asBooleanEvaluation(parameter1), asBooleanEvaluation(parameter2));
    }

    @SuppressWarnings("unchecked")
    private Expression<?> decodeFunction(final DecodeContext context, final ByteBuffer byteBuffer) {
      final Identifier functionName = Identifier.of(this.decodeString(byteBuffer));
      final int functionParameterCount = byteBuffer.getInt();
      final Evaluation<?>[] functionParameters = getFunctionParametersTemplate(functionParameterCount);
      try {
        for (int index = 0; index < functionParameterCount; index++) {
          final Expression<?> functionParameter = this.decode(context, byteBuffer);
          if (!(functionParameter instanceof Evaluation)) {
            // TODO
            throw new UnsupportedOperationException();
          }
          functionParameters[index] = (Evaluation<?>) functionParameter;
        }
        final Function.Declare functionDeclare;
        try {
          functionDeclare = context.getFunctionManager().getFunctionDeclare(functionName, functionParameters);
        } catch (FunctionAmbiguousException exception) {
          throw new RuntimeException(exception);
        }
        if (functionDeclare == null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        return functionDeclare.newInstance(functionParameters);
      } finally {
        Arrays.fill(functionParameters, null);
      }
    }

    @SuppressWarnings("unchecked")
    private static Evaluation<Boolean> asBooleanEvaluation(Expression<?> expression) {
      if (!(expression instanceof Evaluation) || !expression.getResultClass().equals(Boolean.class)) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return (Evaluation<Boolean>) expression;
    }

  }

}
