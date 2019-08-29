package com.huawei.cloudtable.leo.expression;

import com.huawei.cloudtable.leo.Expression;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueConverter;
import com.huawei.cloudtable.leo.common.CollectionHelper;
import com.huawei.cloudtable.leo.common.ExtensionCollector;

import javax.annotation.Nonnull;
import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.math.BigInteger;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class FunctionManager {

  public static final FunctionManager BUILD_IN;

  private static final ThreadLocal<ArrayReader> ARRAY_READER_CACHE = new ThreadLocal<ArrayReader>() {
    @Override
    protected ArrayReader initialValue() {
      return new ArrayReader();
    }
  };

  static {
    final Iterator<Class<? extends ValueConverter>> buildInValueConverterClasses = ExtensionCollector.getExtensionClasses(ValueConverter.class);
    final List<ValueConverter<?, ?>> buildInValueConverterList = new ArrayList<>();
    while (buildInValueConverterClasses.hasNext()) {
      try {
        buildInValueConverterList.add(buildInValueConverterClasses.next().newInstance());
      } catch (InstantiationException | IllegalAccessException exception) {
        throw new RuntimeException(exception);
      }
    }
    final Iterator<Class<? extends Function>> buildInFunctionClasses = ExtensionCollector.getExtensionClasses(Function.class);
    final FunctionManager functionManager = new FunctionManager();
    for (ValueConverter<?, ?> buildInValueConverter : buildInValueConverterList) {
      functionManager.registerConverter(buildInValueConverter);
    }
    while (buildInFunctionClasses.hasNext()) {
      final Class<? extends Function> buildInFunctionClass = buildInFunctionClasses.next();
      if (Expression.class.isAssignableFrom(buildInFunctionClass)) {
        functionManager.registerFunctionDeclares0(buildInFunctionClass);
      } else {
        // TODO
        throw new UnsupportedOperationException(buildInFunctionClass.getName());
      }
    }
    BUILD_IN = functionManager;
  }

  public FunctionManager() {
    this.converterManager = new ConverterManager();
    this.implicitConverterManager = new ImplicitConverterManager();
    this.functionDeclareManager = new FunctionDeclareManager();
  }

  private ConverterManager converterManager;

  private ImplicitConverterManager implicitConverterManager;

  private FunctionDeclareManager functionDeclareManager;

  private static final class ArrayReader extends AbstractList<Evaluation<?>> {

    private Evaluation<?>[] array;

    @Override
    public Evaluation<?> get(final int index) {
      return this.array[index];
    }

    @Override
    public int size() {
      return this.array.length;
    }

    void set(final Evaluation<?>[] array) {
      this.array = array;
    }

  }

  private static final class ConverterKey {

    ConverterKey(final ValueConverter converter) {
      this(converter.getSourceClass(), converter.getTargetClass());
    }

    ConverterKey(final Class<?> sourceClass, final Class<?> targetClass) {
      this.sourceClass = sourceClass;
      this.targetClass = targetClass;
    }

    final Class<?> sourceClass;

    final Class<?> targetClass;

    ConverterKey invert() {
      return new ConverterKey(this.targetClass, this.sourceClass);
    }

    @Override
    public int hashCode() {
      return this.sourceClass.hashCode() * 31 + this.targetClass.hashCode();
    }

    @Override
    public boolean equals(final Object object) {
      if (object == this) {
        return true;
      }
      if (object == null) {
        return false;
      }
      if (object.getClass() == this.getClass()) {
        final ConverterKey that = (ConverterKey) object;
        return this.sourceClass.equals(that.sourceClass) && this.targetClass.equals(that.targetClass);
      } else {
        return false;
      }
    }

  }

  private static final class ConverterManager {

    ConverterManager() {
      this.map = new HashMap<>(0);
    }

    private ConverterManager(final Map<Class<?>, Map<Class<?>, ValueConverter<?, ?>>> map) {
      this.map = map;
    }

    private final Map<Class<?>, Map<Class<?>, ValueConverter<?, ?>>> map;

    @SuppressWarnings("unchecked")
    <TSource, TTarget> ValueConverter<TSource, TTarget> get(final Class<TSource> sourceClass, final Class<TTarget> targetClass) {
      final Map<Class<?>, ValueConverter<?, ?>> mapByTarget = this.map.get(sourceClass);
      if (mapByTarget == null) {
        return null;
      } else {
        return (ValueConverter<TSource, TTarget>) mapByTarget.get(targetClass);
      }
    }

    @SuppressWarnings("unchecked")
    void put(final ValueConverter converter) {
      final ValueConverter existedConverter = this.get(converter.getSourceClass(), converter.getTargetClass());
      if (existedConverter != null) {
        // TODO conflicted.
        throw new UnsupportedOperationException();
      }
      Map<Class<?>, ValueConverter<?, ?>> mapByTarget = this.map.get(converter.getSourceClass());
      if (mapByTarget == null) {
        mapByTarget = new HashMap<>(1);
        this.map.put(converter.getSourceClass(), mapByTarget);
      }
      mapByTarget.put(converter.getTargetClass(), converter);
    }

    ConverterManager duplicate() {
      final Map<Class<?>, Map<Class<?>, ValueConverter<?, ?>>> clonedMap = new HashMap<>(this.map.size());
      for (Map.Entry<Class<?>, Map<Class<?>, ValueConverter<?, ?>>> mapEntry : this.map.entrySet()) {
        clonedMap.put(mapEntry.getKey(), new HashMap<>(mapEntry.getValue()));
      }
      return new ConverterManager(clonedMap);
    }

  }

  public static final class ImplicitConverter<TSource, TTarget> extends ValueConverter<TSource, TTarget> {

    ImplicitConverter(final ValueConverter<TSource, TTarget> converter) {
      super(converter.getSourceClass(), converter.getTargetClass());
      this.key = new ConverterKey(this);
      this.convertStep = 1;
      this.converter = converter;
    }

    <TMedial> ImplicitConverter(
        final ImplicitConverter<TSource, TMedial> converter1,
        final ImplicitConverter<TMedial, TTarget> converter2
    ) {
      super(converter1.getSourceClass(), converter2.getTargetClass());
      this.key = new ConverterKey(this);
      this.convertStep = converter1.convertStep + converter2.convertStep;
      this.converter = new ValueConverter<TSource, TTarget>(converter1.getSourceClass(), converter2.getTargetClass()) {
        @Override
        public TTarget convert(TSource source) {
          return converter2.convert(converter1.convert(source));
        }
      };
    }

    private ImplicitConverter(
        final ConverterKey key,
        final int convertStep,
        final ValueConverter<TSource, TTarget> converter
    ) {
      super(converter.getSourceClass(), converter.getTargetClass());
      this.key = key;
      this.convertStep = convertStep;
      this.converter = converter;
    }

    final ConverterKey key;

    final int convertStep;

    final ValueConverter<TSource, TTarget> converter;

    private Integer priority;

    @Override
    public TTarget convert(TSource source) {
      return this.converter.convert(source);
    }

    /**
     * Starting with 1.
     */
    Integer getPriority() {
      return this.priority;
    }

    void setPriority(final int newPriority) {
      this.priority = newPriority;
    }

    ImplicitConverter<TSource, TTarget> duplicate() {
      return new ImplicitConverter<>(this.key, this.convertStep, this.converter);
    }

  }

  /**
   * Map key is the source class.
   */
  private static final class ImplicitConverterManager {

    ImplicitConverterManager() {
      this.map = new HashMap<>();
      this.mapBySource = new HashMap<>();
      this.mapByTarget = new HashMap<>();
      this.valueClasses = new HashSet<>();
    }

    private final Map<ConverterKey, ImplicitConverter> map;

    private final Map<Class<?>, ImplicitConverterMapByTarget> mapBySource;

    private final Map<Class<?>, ImplicitConverterMapBySource> mapByTarget;

    private final Set<Class<?>> valueClasses;

    private Set<Class<?>> getValueClasses() {
      return Collections.unmodifiableSet(this.valueClasses);
    }

    private Collection<ImplicitConverter> getAll() {
      return this.map.values();
    }

    ImplicitConverterMapByTarget getBySourceClass(final Class<?> sourceClass) {
      return this.mapBySource.get(sourceClass);
    }

    private ImplicitConverterMapBySource getByTargetClass(final Class<?> targetClass) {
      return this.mapByTarget.get(targetClass);
    }

    ImplicitConverter get(final Class<?> sourceClass, final Class<?> targetClass) {
      final ImplicitConverterMapByTarget mapByTarget = this.getBySourceClass(sourceClass);
      return mapByTarget == null ? null : mapByTarget.get(targetClass);
    }

    private ImplicitConverter get(final ConverterKey key) {
      return this.get(key.sourceClass, key.targetClass);
    }

    void put(final ImplicitConverter converter) {
      final ImplicitConverter existedConverter = this.get(converter.getSourceClass(), converter.getTargetClass());
      if (existedConverter != null) {
        if (existedConverter.convertStep <= converter.convertStep) {
          // TODO conflicted.
          throw new UnsupportedOperationException();
        }
      } else {
        if (this.containsKey(converter.key.invert())) {
          // TODO 隐式转图中出现环了
          throw new UnsupportedOperationException();
        }
      }

      this.put0(converter);

      this.updateImplicitConverterChain(converter);
      final ImplicitConverterMapBySource converterMapBySource = this.getByTargetClass(converter.getSourceClass());
      if (converterMapBySource != null) {
        for (ImplicitConverter prevConverter : converterMapBySource.getAll()) {
          this.updateImplicitConverterChain(prevConverter);
        }
      }
    }

    private void put0(final ImplicitConverter converter) {
      if (this.map.put(converter.key, converter) == null) {
        ImplicitConverterMapByTarget mapByTarget = this.mapBySource.get(converter.getSourceClass());
        if (mapByTarget == null) {
          mapByTarget = new ImplicitConverterMapByTarget();
          this.mapBySource.put(converter.getSourceClass(), mapByTarget);
        }
        mapByTarget.put(converter);

        ImplicitConverterMapBySource mapBySource = this.mapByTarget.get(converter.getTargetClass());
        if (mapBySource == null) {
          mapBySource = new ImplicitConverterMapBySource();
          this.mapByTarget.put(converter.getTargetClass(), mapBySource);
        }
        mapBySource.put(converter);
        this.valueClasses.add(converter.getSourceClass());
        this.valueClasses.add(converter.getTargetClass());
      } else {
        this.mapBySource.get(converter.getSourceClass()).put(converter);
        this.mapByTarget.get(converter.getTargetClass()).put(converter);
      }
    }

    private boolean containsKey(final ConverterKey key) {
      return this.get(key) != null;
    }

    ImplicitConverterManager duplicate() {
      final ImplicitConverterManager clone = new ImplicitConverterManager();
      for (ImplicitConverter converter : this.getAll()) {
        clone.put0(converter.duplicate());
      }
      return clone;
    }

    void updateAllConverterPriority() {
      for (ImplicitConverter converter : this.getAll()) {
        converter.setPriority(1);
      }
      for (Class<?> sourceClass : this.getValueClasses()) {
        final ImplicitConverterMapByTarget mapByTarget = this.getBySourceClass(sourceClass);
        if (mapByTarget == null) {
          continue;
        }
        for (Class<?> targetClass : this.getValueClasses()) {
          if (targetClass.equals(sourceClass)) {
            continue;
          }
          final ImplicitConverterMapByTarget innerMapByTarget = this.getBySourceClass(targetClass);
          if (innerMapByTarget == null) {
            continue;
          }
          final ImplicitConverter prevConverter = innerMapByTarget.get(sourceClass);
          if (prevConverter == null) {
            continue;
          }
          for (ImplicitConverter converter : innerMapByTarget.getAll()) {
            final ImplicitConverter innerConverter = mapByTarget.get(converter.getTargetClass());
            if (innerConverter == null) {
              continue;
            }
            final int newPriority = prevConverter.getPriority() + innerConverter.getPriority();
            if (converter.getPriority() < newPriority) {
              converter.setPriority(newPriority);
            }
          }
        }
      }
      for (Class<?> sourceClass : this.getValueClasses()) {
        final ImplicitConverterMapByTarget mapByTarget = this.getBySourceClass(sourceClass);
        if (mapByTarget == null) {
          continue;
        }
        mapByTarget.getConverterListOrderByPriority(true);
      }
    }

    @SuppressWarnings("unchecked")
    private void updateImplicitConverterChain(final ImplicitConverter firstConverter) {
      final ImplicitConverterMapByTarget converterMapByTarget = this.getBySourceClass(firstConverter.getTargetClass());
      if (converterMapByTarget == null) {
        return;
      }
      for (ImplicitConverter nextConverter : converterMapByTarget.getConverterListOrderByPriority()) {
        final ImplicitConverter newConverter = new ImplicitConverter(firstConverter, nextConverter);
        if (this.containsKey(newConverter.key.invert())) {
          // TODO 隐式转图中出现环了
          throw new UnsupportedOperationException();
        }
        final ImplicitConverter<?, ?> existedConverter = this.get(newConverter.key);
        if (existedConverter == null) {
          this.put0(newConverter);
          updateImplicitConverterChain(newConverter);
        } else if (existedConverter.convertStep > newConverter.convertStep) {
          this.put0(newConverter);
          updateImplicitConverterChain(newConverter);
        }
      }
    }

  }

  private static final class ImplicitConverterMapBySource {

    ImplicitConverterMapBySource() {
      this.map = new HashMap<>();
    }

    private final Map<Class<?>, ImplicitConverter> map;

    Collection<ImplicitConverter> getAll() {
      return this.map.values();
    }

    void put(final ImplicitConverter converter) {
      this.map.put(converter.getSourceClass(), converter);
    }

  }

  private static final class ImplicitConverterMapByTarget {

    ImplicitConverterMapByTarget() {
      this.map = new HashMap<>();
      this.converterListOrderByPriority = new ArrayList<>();
    }

    private final Map<Class<?>, ImplicitConverter> map;

    private final List<ImplicitConverter> converterListOrderByPriority;

    List<ImplicitConverter> getConverterListOrderByPriority() {
      return this.getConverterListOrderByPriority(false);
    }

    List<ImplicitConverter> getConverterListOrderByPriority(final boolean reorder) {
      if (reorder) {
        Collections.sort(this.converterListOrderByPriority, ImplicitConverterComparatorByPriority.INSTANCE);
      }
      return this.converterListOrderByPriority;
    }

    Collection<ImplicitConverter> getAll() {
      return this.map.values();
    }

    ImplicitConverter get(final Class<?> targetClass) {
      return this.map.get(targetClass);
    }

    void put(final ImplicitConverter converter) {
      ImplicitConverter existedConverter = this.map.put(converter.getTargetClass(), converter);
      if (existedConverter != null) {
        this.converterListOrderByPriority.remove(existedConverter);
      }
      this.converterListOrderByPriority.add(converter);
    }

  }

  private static final class ImplicitConverterComparatorByPriority implements Comparator<ImplicitConverter> {

    static final ImplicitConverterComparatorByPriority INSTANCE = new ImplicitConverterComparatorByPriority();

    @Override
    public int compare(final ImplicitConverter converter1, final ImplicitConverter converter2) {
      return converter1.getPriority() - converter2.getPriority();
    }

  }

  @SuppressWarnings("unchecked")
  public <TSource, TTarget> ImplicitConverter<TSource, TTarget> getImplicitConverter(
      @Nonnull final Class<TSource> sourceClass,
      @Nonnull final Class<TTarget> targetClass
  ) {
    return this.implicitConverterManager.get(sourceClass, targetClass);
  }

  @SuppressWarnings("unchecked")
  public <TSource, TTarget> ValueConverter<TSource, TTarget> getConverter(
      @Nonnull final Class<TSource> sourceClass,
      @Nonnull final Class<TTarget> targetClass
  ) {
    ValueConverter<TSource, TTarget> converter = this.converterManager.get(sourceClass, targetClass);
    if (converter == null) {
      converter = this.implicitConverterManager.get(sourceClass, targetClass);
    }
    return converter;
  }

  public Function.Declare<?, ?> getFunctionDeclare(final Identifier functionName, final Evaluation<?>... functionParameters)
      throws FunctionAmbiguousException {
    final ArrayReader arrayReader = ARRAY_READER_CACHE.get();
    arrayReader.set(functionParameters);
    return this.functionDeclareManager.get(functionName, arrayReader, this.implicitConverterManager);
  }

  public Function.Declare<?, ?> getFunctionDeclare(final Identifier functionName, final List<Evaluation<?>> functionParameters)
      throws FunctionAmbiguousException {
    return this.functionDeclareManager.get(functionName, functionParameters, this.implicitConverterManager);
  }

  public <TValueConverter extends ValueConverter> void registerConverter(
      final TValueConverter valueConverter
  ) {
    this.registerConverters(Collections.singleton(valueConverter));
  }

  @SuppressWarnings("unchecked")
  public <TValueConverter extends ValueConverter> void registerConverters(
      final Collection<TValueConverter> valueConverters
  ) {
    while (true) {
      final ConverterManager converterManager = this.converterManager;
      final ImplicitConverterManager implicitConverterManager = this.implicitConverterManager;
      final ConverterManager newConverterManager = converterManager.duplicate();
      final ImplicitConverterManager newImplicitConverterManager = implicitConverterManager.duplicate();
      for (TValueConverter valueConverter : valueConverters) {
        newConverterManager.put(valueConverter);
        if (valueConverter instanceof ValueConverter.Implicit) {
          newImplicitConverterManager.put(new ImplicitConverter(valueConverter));
        }
      }
      newImplicitConverterManager.updateAllConverterPriority();
      synchronized (this) {
        if (this.converterManager == converterManager) {
          this.converterManager = converterManager;
          this.implicitConverterManager = newImplicitConverterManager;
          return;
        }
      }
    }
  }

  public void registerFunctionDeclare(final Function.Declare<?, ?> functionDeclare) {
    this.registerFunctionDeclares(Collections.singleton(functionDeclare));
  }

  public void registerFunctionDeclares(final Collection<Function.Declare<?, ?>> functionDeclares) {
    while (true) {
      final FunctionDeclareManager declareManager = this.functionDeclareManager;
      final FunctionDeclareManager newDeclareManager = declareManager.duplicate();
      for (Function.Declare<?, ?> functionDeclare : functionDeclares) {
        newDeclareManager.put(functionDeclare);
      }
      for (Map.Entry<Identifier, FunctionDeclareMapByParameterCount> entry : newDeclareManager.getAll().entrySet()) {
        entry.getValue().get(true);
        entry.getValue().refreshChildList();
      }
      synchronized (this) {
        if (this.functionDeclareManager == declareManager) {
          this.functionDeclareManager = newDeclareManager;
          return;
        }
      }
    }
  }

  @SuppressWarnings("unchecked")
  public <TFunction extends Expression & Function> void registerFunctionDeclares(
      final Class<TFunction> functionClass
  ) {
    if (functionClass.isArray()) {
      throw new IllegalArgumentException("Argument [functionClass] is an array.");
    }
    if (functionClass.isInterface()) {
      throw new IllegalArgumentException("Argument [functionClass] is an interface.");
    }
    if (Modifier.isAbstract(functionClass.getModifiers())) {
      throw new IllegalArgumentException("Argument [functionClass] is an abstract class.");
    }
    final Constructor<?>[] functionConstructors = functionClass.getConstructors();
    final List<Function.Declare<?, ?>> functionDeclareList = new ArrayList<>(functionConstructors.length);
    for (Constructor<?> functionConstructor : functionConstructors) {
      final Function.Declare<?, ?> functionDeclare;
      try {
        functionDeclare = Function.getDeclare((Constructor<TFunction>) functionConstructor);
      } catch (FunctionDeclareException exception) {
        // TODO log warning.
        throw new UnsupportedOperationException(exception);
      }
      functionDeclareList.add(functionDeclare);
    }
    this.registerFunctionDeclares(functionDeclareList);
  }

  @SuppressWarnings("unchecked")
  private void registerFunctionDeclares0(final Class functionClass) {
    if (!Expression.class.isAssignableFrom(functionClass)) {
      // TODO
      throw new UnsupportedOperationException(functionClass.getName());
    }
    if (!Function.class.isAssignableFrom(functionClass)) {
      // TODO
      throw new UnsupportedOperationException(functionClass.getName());
    }
    this.registerFunctionDeclares(functionClass);
  }

  public <TSource, TTarget> Casting<TSource, TTarget> buildCastingFunction(
      final Evaluation<TSource> sourceExpression,
      final Class<TTarget> targetClass
  ) {
    if (sourceExpression == null) {
      throw new IllegalArgumentException("Argument [sourceExpression] is null.");
    }
    if (targetClass == null) {
      throw new IllegalArgumentException("Argument [targetClass] is null.");
    }
    final ImplicitConverter<TSource, TTarget> valueConverter =
        this.getImplicitConverter(sourceExpression.getResultClass(), targetClass);
    if (valueConverter == null) {
      return null;
    }
    return new Casting<>(sourceExpression, valueConverter);
  }

  private static final class FunctionDeclareManager {

    private static final int VAR_PARAMETERS_COUNT = -1;

    private static final ThreadLocal<FunctionDeclareMatchesRecorder> MATCHES_RECORDER_CACHE = new ThreadLocal<FunctionDeclareMatchesRecorder>() {
      @Override
      protected FunctionDeclareMatchesRecorder initialValue() {
        return new FunctionDeclareMatchesRecorder();
      }
    };

    FunctionDeclareManager() {
      this.mapByName = new HashMap<>();
      this.declareList = new ArrayList<>();
      this.arrayClassMapByElementClass = new HashMap<>();
    }

    private final Map<Identifier, FunctionDeclareMapByParameterCount> mapByName;

    private final List<Function.Declare<?, ?>> declareList;

    private final Map<Class<?>, Class<?>> arrayClassMapByElementClass;

    private boolean contains(final Identifier functionName, final List<Class<?>> functionParameterClassList) {
      return this.get(functionName, functionParameterClassList) != null;
    }

    Map<Identifier, FunctionDeclareMapByParameterCount> getAll() {
      return Collections.unmodifiableMap(this.mapByName);
    }

    private Function.Declare<?, ?> get(final Identifier functionName, final List<Class<?>> functionParameterClassList) {
      final FunctionDeclareMapByParameterCount mapByParameterCount = this.mapByName.get(functionName);
      if (mapByParameterCount == null) {
        return null;
      }
      if (functionParameterClassList.isEmpty() || !CollectionHelper.getLast(functionParameterClassList).isArray()) {
        return getFixParasDeclareWithClassList(mapByParameterCount, functionParameterClassList);
      } else {
        return getVarParasDeclareWithClassList(mapByParameterCount, functionParameterClassList);
      }
    }

    /**
     * Get the matched function declare.
     */
    Function.Declare<?, ?> get(
        final Identifier functionName,
        final List<Evaluation<?>> functionParameters,
        final ImplicitConverterManager implicitConverterManager
    ) throws FunctionAmbiguousException {
      final FunctionDeclareMapByParameterCount mapByParameterCount = this.mapByName.get(functionName);
      if (mapByParameterCount == null) {
        return null;
      }

      Function.Declare<?, ?> functionDeclare;

      // TODO 经过隐藏式转换，找到最佳，立即缓存
      // The only declare.
      functionDeclare = mapByParameterCount.get();
      if (functionDeclare != null) {
        try {
          if (matches(functionDeclare, functionParameters, implicitConverterManager)) {
            return functionDeclare;
          }
        } catch (FunctionMatchException exception) {
          // TODO
          throw new UnsupportedOperationException();
        }
      }
      // No parameters.
      if (functionParameters.isEmpty()) {
        functionDeclare = getFixParasDeclareWithEvaluationList(mapByParameterCount, functionParameters);
        if (functionDeclare != null) {
          return functionDeclare;
        } else {
          return getVarParasDeclareWithEvaluationList(mapByParameterCount, functionParameters, implicitConverterManager);
        }
      }
      // Fix parameters.
      List<Function.Declare<?, ?>> ambiguousFunctionDeclareList = null;
      functionDeclare = getFixParasDeclareWithEvaluationList(mapByParameterCount, functionParameters);
      if (functionDeclare != null) {
        return functionDeclare;
      }
      try {
        functionDeclare = getFixParasDeclareWithEvaluationList(mapByParameterCount, functionParameters, implicitConverterManager);
        if (functionDeclare != null) {
          return functionDeclare;
        }
      } catch (FunctionAmbiguousException exception) {
        ambiguousFunctionDeclareList = new ArrayList<>(exception.getFunctionDeclareList());
      }
      // Var parameters.
      functionDeclare = getVarParasDeclareWithEvaluationList(mapByParameterCount, functionParameters, this.arrayClassMapByElementClass);
      if (functionDeclare != null) {
        return functionDeclare;
      }
      functionDeclare = getVarParasDeclareWithEvaluationList(mapByParameterCount, functionParameters, implicitConverterManager);
      if (functionDeclare != null) {
        return functionDeclare;
      }

      if (ambiguousFunctionDeclareList != null) {
        throw new FunctionAmbiguousException(ambiguousFunctionDeclareList);
      }
      // TODO throw specific exception. nullable.
      return null;
    }

    private static Function.Declare<?, ?> getFixParasDeclareWithClassList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Class<?>> functionParameterClassList
    ) {
      FunctionDeclareTreeNode functionDeclareTreeNode;
      if (functionParameterClassList.isEmpty()) {
        functionDeclareTreeNode = functionDeclareMapByParameterCount.getRoot(0);
      } else {
        final int functionParameterCount = functionParameterClassList.size();
        int functionParameterIndex = 0;
        functionDeclareTreeNode = functionDeclareMapByParameterCount.getRoot(functionParameterCount);
        for (; functionParameterIndex < functionParameterCount; functionParameterIndex++) {
          if (functionDeclareTreeNode == null) {
            break;
          }
          functionDeclareTreeNode = functionDeclareTreeNode.getChild(functionParameterClassList.get(functionParameterIndex));
        }
      }
      if (functionDeclareTreeNode != null) {
        return functionDeclareTreeNode.get();
      } else {
        return null;
      }
    }

    /**
     * 参数个数确定，参数个数、类型完全匹配
     * 最高分
     */
    private static Function.Declare<?, ?> getFixParasDeclareWithEvaluationList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Evaluation<?>> functionParameters
    ) {
      FunctionDeclareTreeNode functionDeclareTreeNode;
      if (functionParameters.size() == 0) {
        functionDeclareTreeNode = functionDeclareMapByParameterCount.getRoot(0);
      } else {
        final int functionParameterCount = functionParameters.size();
        int functionParameterIndex = 0;
        functionDeclareTreeNode = functionDeclareMapByParameterCount.getRoot(functionParameterCount);
        for (; functionParameterIndex < functionParameterCount; functionParameterIndex++) {
          if (functionDeclareTreeNode == null) {
            return null;
          }
          final Evaluation<?> functionParameter = functionParameters.get(functionParameterIndex);
          if (functionParameter == null) {
            return null;
          }
          functionDeclareTreeNode = functionDeclareTreeNode.getChild(functionParameter.getResultClass());
        }
      }
      if (functionDeclareTreeNode != null) {
        return functionDeclareTreeNode.get();
      } else {
        return null;
      }
    }

    /**
     * 参数个数确定，参数个数、类型（隐式转换后）完全匹配
     * 采取打分制，如果最高分不唯一，则匹配失败，抛提示异常，外部需要处理异常
     */
    private static Function.Declare<?, ?> getFixParasDeclareWithEvaluationList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Evaluation<?>> functionParameters,
        final ImplicitConverterManager implicitConverterManager
    ) throws FunctionAmbiguousException {
      // Count build function parameter can not be zero.
      final int functionParameterCount = functionParameters.size();
      final FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(functionParameterCount);
      if (functionDeclareTreeRoot == null) {
        return null;
      }
      final FunctionDeclareMatchesRecorder matchesRecorder = MATCHES_RECORDER_CACHE.get();
      matchesRecorder.reset();
      getFixParasDeclareWithEvaluationList(
          functionDeclareTreeRoot,
          0,
          functionParameters,
          matchesRecorder,
          implicitConverterManager,
          Integer.MAX_VALUE
      );
      final List<Function.Declare<?, ?>> matchedFunctionDeclareList = matchesRecorder.getMatchedFunctionDeclareList();
      switch (matchedFunctionDeclareList.size()) {
        case 0:
          return null;
        case 1:
          return matchedFunctionDeclareList.get(0);
        default:
          throw new FunctionAmbiguousException(matchesRecorder.getMatchedFunctionDeclareList());
      }
    }

    private static void getFixParasDeclareWithEvaluationList(
        final FunctionDeclareTreeNode functionDeclareTreeNode,
        final int functionParameterIndex,
        final List<Evaluation<?>> functionParameters,
        final FunctionDeclareMatchesRecorder matchesRecorder,
        final ImplicitConverterManager implicitConverterManager,
        int cumulativeScore
    ) {
      if (functionDeclareTreeNode.isLeaf()) {
        matchesRecorder.record(cumulativeScore, functionDeclareTreeNode.get());
        return;
      }
      final Evaluation<?> parameter = functionParameters.get(functionParameterIndex);
      final int nextParameterIndex = functionParameterIndex + 1;
      if (parameter == null) {
        //  TODO 在这里检查required？
        final List<FunctionDeclareTreeNode> children = functionDeclareTreeNode.getChildren();
        for (int index = 0; index < children.size(); index++) {
          getFixParasDeclareWithEvaluationList(
              children.get(index),
              nextParameterIndex,
              functionParameters,
              matchesRecorder,
              implicitConverterManager,
              cumulativeScore
          );
        }
        return;
      }
      // self -> ... -> Object
      final Class<?> parameterClass = parameter.getResultClass();
      FunctionDeclareTreeNode functionDeclareTreeChild;
      // self
      functionDeclareTreeChild = functionDeclareTreeNode.getChild(parameterClass);
      if (functionDeclareTreeChild != null) {
        getFixParasDeclareWithEvaluationList(
            functionDeclareTreeChild,
            nextParameterIndex,
            functionParameters,
            matchesRecorder,
            implicitConverterManager,
            cumulativeScore
        );
      }
      // ...
      final ImplicitConverterMapByTarget implicitConverterMapByTarget = implicitConverterManager.getBySourceClass(parameterClass);
      if (implicitConverterMapByTarget != null) {
        int currentPriority = 0;
        final List<ImplicitConverter> implicitConverterList = implicitConverterMapByTarget.getConverterListOrderByPriority();
        for (int index = 0; index < implicitConverterList.size(); index++) {
          final ImplicitConverter implicitConverter = implicitConverterList.get(index);
          functionDeclareTreeChild = functionDeclareTreeNode.getChild(implicitConverter.getTargetClass());
          if (functionDeclareTreeChild != null) {
            if (implicitConverter.getPriority() != currentPriority) {
              cumulativeScore -= 1;
              if (cumulativeScore < matchesRecorder.getScore()) {
                return;
              }
              currentPriority = implicitConverter.getPriority();
            }
            getFixParasDeclareWithEvaluationList(
                functionDeclareTreeChild,
                nextParameterIndex,
                functionParameters,
                matchesRecorder,
                implicitConverterManager,
                cumulativeScore
            );
          }
        }
      }
      // Object
      functionDeclareTreeChild = functionDeclareTreeNode.getChild(Object.class);
      if (functionDeclareTreeChild != null) {
        cumulativeScore -= 1;
        if (cumulativeScore < matchesRecorder.getScore()) {
          return;
        }
        getFixParasDeclareWithEvaluationList(
            functionDeclareTreeChild,
            nextParameterIndex,
            functionParameters,
            matchesRecorder,
            implicitConverterManager,
            cumulativeScore
        );
      }
    }

    private static Function.Declare<?, ?> getVarParasDeclareWithClassList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Class<?>> functionParameterClassList
    ) {
      final FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(VAR_PARAMETERS_COUNT);
      if (functionDeclareTreeRoot == null) {
        return null;
      }
      final AtomicBoolean allowBack = ALLOW_BACK_MARK_CACHE.get();
      allowBack.set(true);
      return getVarParasDeclareWithClassList(
          functionDeclareTreeRoot,
          functionParameterClassList,
          0,
          allowBack
      );
    }

    private static final ThreadLocal<AtomicBoolean> ALLOW_BACK_MARK_CACHE = new ThreadLocal<AtomicBoolean>() {
      @Override
      protected AtomicBoolean initialValue() {
        return new AtomicBoolean(true);
      }
    };

    private static Function.Declare<?, ?> getVarParasDeclareWithClassList(
        final FunctionDeclareTreeNode functionDeclareTreeNode,
        final List<Class<?>> functionParameterClassList,
        final int functionParameterIndex,
        final AtomicBoolean allowBack
    ) {
      final Class<?> functionParameterClass = functionParameterClassList.get(functionParameterIndex);
      FunctionDeclareTreeNode functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterClass);
      if (functionDeclareTreeChild != null) {
        if (functionDeclareTreeChild.isLeaf()) {
          return functionDeclareTreeChild.get();
        } else {
          final Function.Declare functionDeclare = getVarParasDeclareWithClassList(
              functionDeclareTreeChild,
              functionParameterClassList,
              functionParameterIndex + 1,
              allowBack
          );
          if (functionDeclare != null) {
            return functionDeclare;
          }
        }
      } else if (functionParameterIndex == functionParameterClassList.size() - 1) {// Is the last parameter index.
        // 往下找
        final Class<?> theLastFunctionParameterClass = CollectionHelper.getLast(functionParameterClassList);
        functionDeclareTreeChild = functionDeclareTreeNode.getChild(theLastFunctionParameterClass.getComponentType());
        while (functionDeclareTreeChild != null) {
          final FunctionDeclareTreeNode functionDeclareTreeLeaf = functionDeclareTreeChild.getChild(theLastFunctionParameterClass);
          if (functionDeclareTreeLeaf != null) {
            return functionDeclareTreeLeaf.get();
          }
          functionDeclareTreeChild = functionDeclareTreeChild.getChild(theLastFunctionParameterClass.getComponentType());
        }
        return null;
      }
      if (allowBack.get()) {
        // 往上找
        final Class<?> theLastFunctionParameterClass = CollectionHelper.getLast(functionParameterClassList);
        if (functionParameterClass.equals(theLastFunctionParameterClass.getComponentType())) {
          final FunctionDeclareTreeNode functionDeclareTreeLeaf = functionDeclareTreeNode.getChild(theLastFunctionParameterClass);
          if (functionDeclareTreeLeaf != null) {
            return functionDeclareTreeLeaf.get();
          }
        } else {
          allowBack.set(false);
        }
      }
      return null;
    }

    /**
     * 参数个数不确定，参数类型完全匹配
     */
    private static Function.Declare<?, ?> getVarParasDeclareWithEvaluationList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Evaluation<?>> functionParameters,
        final Map<Class<?>, Class<?>> arrayClassMapByElementClass
    ) {
      final FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(VAR_PARAMETERS_COUNT);
      if (functionDeclareTreeRoot == null) {
        return null;
      }
      final AtomicBoolean allowBack = ALLOW_BACK_MARK_CACHE.get();
      allowBack.set(true);
      return getVarParasDeclareWithEvaluationList(
          functionDeclareTreeRoot,
          functionParameters,
          0,
          arrayClassMapByElementClass,
          allowBack
      );
    }

    private static Function.Declare<?, ?> getVarParasDeclareWithEvaluationList(
        final FunctionDeclareTreeNode functionDeclareTreeNode,
        final List<Evaluation<?>> functionParameters,
        int functionParameterIndex,
        final Map<Class<?>, Class<?>> arrayClassMapByElementClass,
        final AtomicBoolean allowBack
    ) {
      Evaluation<?> functionParameter = functionParameters.get(functionParameterIndex);
      if (functionParameter == null) {
        allowBack.set(false);
        return null;
      }

      final Class<?> functionParameterClass = functionParameter.getResultClass();
      FunctionDeclareTreeNode functionDeclareTreeChild;

      functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterClass);
      if (functionDeclareTreeChild != null && functionParameterIndex != functionParameters.size() - 1) {
        // Is not the last parameter index.
        final Function.Declare functionDeclare = getVarParasDeclareWithEvaluationList(
            functionDeclareTreeChild,
            functionParameters,
            functionParameterIndex + 1,
            arrayClassMapByElementClass,
            allowBack
        );
        if (functionDeclare != null) {
          return functionDeclare;
        }
        if (allowBack.get()) {
          if (functionParameterClass.equals(CollectionHelper.getLast(functionParameters).getResultClass())) {
            final Class<?> functionParameterArrayClass = arrayClassMapByElementClass.get(functionParameterClass);
            if (functionParameterArrayClass == null) {
              return null;
            }
            functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterArrayClass);
            if (functionDeclareTreeChild == null) {
              return null;
            } else {
              return functionDeclareTreeChild.get();
            }
          } else {
            allowBack.set(false);
            return null;
          }
        } else {
          return null;
        }
      }

      final Class<?> functionParameterArrayClass = arrayClassMapByElementClass.get(functionParameterClass);
      if (functionParameterArrayClass == null) {
        return null;
      }
      functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterArrayClass);
      if (functionDeclareTreeChild == null) {
        return null;
      }
      functionParameterIndex += 1;
      for (; functionParameterIndex < functionParameters.size(); functionParameterIndex++) {
        functionParameter = functionParameters.get(functionParameterIndex);
        if (functionParameter == null) {
          allowBack.set(false);
          return null;
        }
        if (!functionParameter.getResultClass().equals(functionParameterClass)) {
          return null;
        }
      }
      return functionDeclareTreeChild.get();
    }

    /**
     * 参数个数不确定，参数类型（隐式转换后）完全匹配
     */
    private static Function.Declare<?, ?> getVarParasDeclareWithEvaluationList(
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount,
        final List<Evaluation<?>> functionParameters,
        final ImplicitConverterManager implicitConverterManager
    ) throws FunctionAmbiguousException {
      final FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(VAR_PARAMETERS_COUNT);
      if (functionDeclareTreeRoot == null) {
        return null;
      }
      final FunctionDeclareMatchesRecorder matchesRecorder = MATCHES_RECORDER_CACHE.get();
      matchesRecorder.reset();
      getVarParasDeclareWithEvaluationList(
          functionDeclareTreeRoot,
          0,
          functionParameters,
          matchesRecorder,
          implicitConverterManager,
          Integer.MAX_VALUE
      );
      final List<Function.Declare<?, ?>> matchedFunctionDeclareList = matchesRecorder.getMatchedFunctionDeclareList();
      switch (matchedFunctionDeclareList.size()) {
        case 0:
          return null;
        case 1:
          return matchedFunctionDeclareList.get(0);
        default:
          throw new FunctionAmbiguousException(matchesRecorder.getMatchedFunctionDeclareList());
      }
    }

    private static void getVarParasDeclareWithEvaluationList(
        final FunctionDeclareTreeNode functionDeclareTreeNode,
        final int functionParameterIndex,
        final List<Evaluation<?>> functionParameters,
        final FunctionDeclareMatchesRecorder matchesRecorder,
        final ImplicitConverterManager implicitConverterManager,
        int cumulativeScore
    ) {
      Class<?> clazz = functionParameters.get(0).getResultClass();
      Class<?> arrayClazz;
      try {
        arrayClazz = Class.forName("[L" + clazz.getCanonicalName() + ";");
      } catch (ClassNotFoundException e) {
        throw new UnsupportedOperationException(e);
      }
      matchesRecorder.record(0, functionDeclareTreeNode.getChild(clazz).getChild(arrayClazz).functionDeclare);
    }

    private static boolean matches(
        final Function.Declare<?, ?> functionDeclare,
        final List<Evaluation<?>> functionParameters,
        final ImplicitConverterManager implicitConverterManager
    ) throws FunctionMatchException {
      final int valueParameterCount = functionDeclare.getValueParameterCount();
      if (functionParameters.size() < valueParameterCount) {
        return false;
      }

      int functionParameterIndex = 0;

      for (; functionParameterIndex < valueParameterCount; functionParameterIndex++) {
        final Evaluation<?> functionParameter = functionParameters.get(functionParameterIndex);
        if (functionParameter == null) {
          if (functionDeclare.getValueParameterDeclare(functionParameterIndex).isRequired()) {
            // TODO 要抛异常 required.
            throw new FunctionMatchException();
          }
          continue;
        }
        final Class<?> valueParameterClass = functionDeclare.getValueParameterDeclare(functionParameterIndex).getClazz();
        if (valueParameterClass == Object.class) {
          continue;
        }
        final Class<?> functionParameterClass = functionParameter.getResultClass();
        if (valueParameterClass.equals(functionParameterClass)) {
          continue;
        }
        if (implicitConverterManager.get(functionParameterClass, valueParameterClass) == null) {
          return false;
        }
      }

      final Function.ArrayParameterDeclare arrayParameterDeclare = functionDeclare.getArrayParameterDeclare();
      if (arrayParameterDeclare != null) {
        if (functionParameters.size() == valueParameterCount) {
          if (arrayParameterDeclare.isRequired()) {
            // TODO 要抛异常 required.
            throw new FunctionMatchException();
          } else {
            return true;
          }
        }
        if (arrayParameterDeclare.getElementClass() == Object.class) {
          if (arrayParameterDeclare.isElementNullable()) {
            return true;
          } else {
            for (; functionParameterIndex < functionParameters.size(); functionParameterIndex++) {
              if (functionParameters.get(functionParameterIndex) == null) {
                // TODO 要抛异常 required.
                throw new FunctionMatchException();
              }
            }
          }
        } else {
          for (; functionParameterIndex < functionParameters.size(); functionParameterIndex++) {
            final Evaluation<?> functionParameter = functionParameters.get(functionParameterIndex);
            if (functionParameter == null) {
              if (!arrayParameterDeclare.isElementNullable()) {
                // TODO 要抛异常 required.
                throw new FunctionMatchException();
              }
              continue;
            }
            final Class<?> functionParameterClass = functionParameter.getResultClass();
            if (arrayParameterDeclare.getElementClass().equals(functionParameterClass)) {
              continue;
            }
            if (implicitConverterManager.get(functionParameterClass, arrayParameterDeclare.getElementClass()) == null) {
              return false;
            }
          }
        }
      }
      return true;
    }

    void put(final Function.Declare<?, ?> functionDeclare) {
      boolean conflicted = this.contains(functionDeclare.getName(), functionDeclare.getParameterClassList());
      if (!conflicted && functionDeclare.isCommutative()) {
        final Class<?> parameter1Class = functionDeclare.getValueParameterDeclare(0).getClazz();
        final Class<?> parameter2Class = functionDeclare.getValueParameterDeclare(1).getClazz();
        if (parameter1Class != parameter2Class) {
          conflicted = this.contains(functionDeclare.getName(), Arrays.asList(parameter2Class, parameter1Class));
        }
      }
      if (conflicted) {
        // TODO conflicted.
        throw new UnsupportedOperationException(functionDeclare.toString());
      }
      this.put0(functionDeclare.getName(), functionDeclare.getParameterClassList(), functionDeclare);
      if (functionDeclare.isCommutative()) {
        final Class<?> parameter1Class = functionDeclare.getValueParameterDeclare(0).getClazz();
        final Class<?> parameter2Class = functionDeclare.getValueParameterDeclare(1).getClazz();
        if (parameter1Class != parameter2Class) {
          this.put0(functionDeclare.getName(), Arrays.asList(parameter2Class, parameter1Class), functionDeclare);
        }
      }
      this.declareList.add(functionDeclare);
    }

    private void put0(
        final Identifier functionName,
        final List<Class<?>> functionParameterClassList,
        final Function.Declare<?, ?> functionDeclare
    ) {
      FunctionDeclareMapByParameterCount mapByParameterCount = this.mapByName.get(functionName);
      if (mapByParameterCount == null) {
        mapByParameterCount = new FunctionDeclareMapByParameterCount();
        this.mapByName.put(functionName, mapByParameterCount);
      }
      if (functionDeclare.getArrayParameterDeclare() == null) {
        putFixParasDeclare(functionParameterClassList, functionDeclare, mapByParameterCount);
      } else {
        putVarParasDeclare(functionParameterClassList, functionDeclare, mapByParameterCount);
        final Class<?> arrayParameterClass = functionDeclare.getArrayParameterDeclare().getClazz();
        this.arrayClassMapByElementClass.put(arrayParameterClass.getComponentType(), arrayParameterClass);
      }
    }

    private static void putFixParasDeclare(
        final List<Class<?>> functionParameterClassList,
        final Function.Declare<?, ?> functionDeclare,
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount
    ) {
      if (functionParameterClassList.isEmpty()) {
        functionDeclareMapByParameterCount.putRoot(0, new FunctionDeclareTreeNode(functionDeclare));
        return;
      }
      final int functionParameterCount = functionParameterClassList.size();
      FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(functionParameterCount);
      if (functionDeclareTreeRoot == null) {
        functionDeclareTreeRoot = new FunctionDeclareTreeNode();
        functionDeclareMapByParameterCount.putRoot(functionParameterCount, functionDeclareTreeRoot);
      }
      FunctionDeclareTreeNode functionDeclareTreeNode = functionDeclareTreeRoot;
      int functionParameterIndex = 0;
      for (; functionParameterIndex < functionParameterCount - 1; functionParameterIndex++) {
        final Class<?> functionParameterClass = functionParameterClassList.get(functionParameterIndex);
        FunctionDeclareTreeNode functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterClass);
        if (functionDeclareTreeChild == null) {
          functionDeclareTreeChild = new FunctionDeclareTreeNode();
          functionDeclareTreeNode.putChild(functionParameterClass, functionDeclareTreeChild);
        }
        functionDeclareTreeNode = functionDeclareTreeChild;
      }
      final Class<?> functionParameterClass = functionParameterClassList.get(functionParameterIndex);
      functionDeclareTreeNode.putChild(functionParameterClass, new FunctionDeclareTreeNode(functionDeclare));
    }

    private static void putVarParasDeclare(
        final List<Class<?>> functionParameterClassList,
        final Function.Declare<?, ?> functionDeclare,
        final FunctionDeclareMapByParameterCount functionDeclareMapByParameterCount
    ) {
      FunctionDeclareTreeNode functionDeclareTreeRoot = functionDeclareMapByParameterCount.getRoot(VAR_PARAMETERS_COUNT);
      if (functionDeclareTreeRoot == null) {
        functionDeclareTreeRoot = new FunctionDeclareTreeNode();
        functionDeclareMapByParameterCount.putRoot(VAR_PARAMETERS_COUNT, functionDeclareTreeRoot);
      }
      final int fixFunctionParameterCount = functionParameterClassList.size() - 1;
      FunctionDeclareTreeNode functionDeclareTreeNode = functionDeclareTreeRoot;
      int functionParameterIndex = 0;
      for (; functionParameterIndex < fixFunctionParameterCount; functionParameterIndex++) {
        final Class<?> functionParameterClass = functionParameterClassList.get(functionParameterIndex);
        FunctionDeclareTreeNode functionDeclareTreeChild = functionDeclareTreeNode.getChild(functionParameterClass);
        if (functionDeclareTreeChild == null) {
          functionDeclareTreeChild = new FunctionDeclareTreeNode();
          functionDeclareTreeNode.putChild(functionParameterClass, functionDeclareTreeChild);
        }
        functionDeclareTreeNode = functionDeclareTreeChild;
      }
      final Class<?> functionParameterClass = functionParameterClassList.get(functionParameterIndex);
      functionDeclareTreeNode.putChild(functionParameterClass, new FunctionDeclareTreeNode(functionDeclare));
    }

    FunctionDeclareManager duplicate() {
      final FunctionDeclareManager newFunctionDeclareManager = new FunctionDeclareManager();
      for (Function.Declare<?, ?> functionDeclare : this.declareList) {
        newFunctionDeclareManager.put(functionDeclare);
      }
      return newFunctionDeclareManager;
    }

  }

  private static final class FunctionDeclareMatchesRecorder {

    FunctionDeclareMatchesRecorder() {
      this.score = Integer.MIN_VALUE;
      this.matchedFunctionDeclareList = new ArrayList<>();
    }

    private int score;

    private final List<Function.Declare<?, ?>> matchedFunctionDeclareList;

    int getScore() {
      return this.score;
    }

    List<Function.Declare<?, ?>> getMatchedFunctionDeclareList() {
      return this.matchedFunctionDeclareList;
    }

    void record(final int score, final Function.Declare<?, ?> functionDeclare) {
      if (score < this.score) {
        throw new RuntimeException();
      }
      if (score > this.score) {
        this.score = score;
        this.matchedFunctionDeclareList.clear();
      }
      this.matchedFunctionDeclareList.add(functionDeclare);
    }

    void reset() {
      this.score = Integer.MIN_VALUE;
      this.matchedFunctionDeclareList.clear();
    }

  }

  private static final class FunctionDeclareMapByParameterCount {

    FunctionDeclareMapByParameterCount() {
      this.map = new HashMap<>();
    }

    private final Map<Integer, FunctionDeclareTreeNode> map;

    private Function.Declare theOnly;

    /**
     * Get the only.
     */
    Function.Declare get() {
      return this.get(false);
    }

    /**
     * Get the only.
     */
    Function.Declare get(final boolean refresh) {
      if (refresh) {
        if (this.map.size() == 1) {
          this.theOnly = get0(this.map.values().iterator().next());
        } else {
          this.theOnly = null;
        }
      }
      return this.theOnly;
    }

    private static Function.Declare get0(final FunctionDeclareTreeNode functionDeclareTreeNode) {
      if (functionDeclareTreeNode.isLeaf()) {
        return functionDeclareTreeNode.get();
      }
      final Collection<FunctionDeclareTreeNode> children = functionDeclareTreeNode.getChildren(true);
      if (children.size() == 1) {
        return get0(children.iterator().next());
      } else {
        return null;
      }
    }

    FunctionDeclareTreeNode getRoot(final int functionParameterCount) {
      return this.map.get(functionParameterCount);
    }

    void putRoot(final int functionParameterCount, final FunctionDeclareTreeNode functionDeclareTreeRoot) {
      this.map.put(functionParameterCount, functionDeclareTreeRoot);
    }

    void refreshChildList() {
      for (FunctionDeclareTreeNode functionDeclareTreeNode : this.map.values()) {
        refreshChildList(functionDeclareTreeNode);
      }
    }

    private static void refreshChildList(final FunctionDeclareTreeNode functionDeclareTreeNode) {
      if (functionDeclareTreeNode.isLeaf()) {
        return;
      }
      for (FunctionDeclareTreeNode functionDeclareTreeChild : functionDeclareTreeNode.getChildren(true)) {
        refreshChildList(functionDeclareTreeChild);
      }
    }

  }

  private static final class FunctionDeclareTreeNode {

    FunctionDeclareTreeNode() {
      this.children = new HashMap<>();
      this.functionDeclare = null;
    }

    FunctionDeclareTreeNode(final Function.Declare functionDeclare) {
      if (functionDeclare == null) {
        throw new IllegalArgumentException("Argument [functionDeclare] is null.");
      }
      this.childList = Collections.emptyList();
      this.children = Collections.emptyMap();
      this.functionDeclare = functionDeclare;
    }

    private List<FunctionDeclareTreeNode> childList;

    private final Map<Class<?>, FunctionDeclareTreeNode> children;

    private final Function.Declare functionDeclare;

    Function.Declare get() {
      return this.functionDeclare;
    }

    FunctionDeclareTreeNode getChild(final Class<?> parameterClass) {
      return this.children.get(parameterClass);
    }

    List<FunctionDeclareTreeNode> getChildren() {
      return this.getChildren(false);
    }

    List<FunctionDeclareTreeNode> getChildren(final boolean refresh) {
      if (refresh && this.functionDeclare == null) {
        this.childList = new ArrayList<>(this.children.values());
      }
      return this.childList;
    }

    void putChild(final Class<?> parameterClass, final FunctionDeclareTreeNode child) {
      this.children.put(parameterClass, child);
    }

    boolean isLeaf() {
      return this.functionDeclare != null;
    }

  }

}
