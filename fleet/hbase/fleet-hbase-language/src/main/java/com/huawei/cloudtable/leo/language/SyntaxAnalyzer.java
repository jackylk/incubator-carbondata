package com.huawei.cloudtable.leo.language;

import org.apache.log4j.Logger;

import java.lang.annotation.Annotation;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;

public final class SyntaxAnalyzer {

  private static final Logger LOGGER = Logger.getLogger(SyntaxAnalyzer.class);

  public static final class Builder {

    @SuppressWarnings("unchecked")
    public Builder(
        final Set<Character> whitespaceSet,
        final boolean caseSensitive,
        final List<Class<? extends SyntaxTree.Node>> nodeClassList
    ) {
      final List<Class<? extends Lexical>> lexicalClassList = new ArrayList<>();
      for (Class<? extends SyntaxTree.Node> nodeClass : nodeClassList) {
        if (Lexical.class.isAssignableFrom(nodeClass)) {
          lexicalClassList.add((Class<? extends Lexical>) nodeClass);
        }
      }
      this.analyzer = new LexicalAnalyzer.Builder(whitespaceSet, caseSensitive, lexicalClassList).build();
      this.nodeAnalyseForest = buildNodeAnalyseForest(nodeClassList);
    }

    private final LexicalAnalyzer analyzer;

    private final NodeAnalyseForest nodeAnalyseForest;

    public SyntaxAnalyzer build() {
      return new SyntaxAnalyzer(this.analyzer, this.nodeAnalyseForest);
    }

    @SuppressWarnings("unchecked")
    private static NodeAnalyseForest buildNodeAnalyseForest(final List<Class<? extends SyntaxTree.Node>> nodeClassList) {
      final Set<Class<? extends SyntaxTree.Node>> nodeClassSet = new HashSet<>(nodeClassList.size());
      for (Class<? extends SyntaxTree.Node> nodeClass : nodeClassList) {
        if (Lexical.class.isAssignableFrom(nodeClass)) {
          continue;
        }
        if (nodeClassSet.contains(nodeClass)) {
          continue;
        }
        final String errorMessage = checkNodeClass(nodeClass);
        if (errorMessage != null) {
          // TODO
          throw new UnsupportedOperationException();
        }
        nodeClassSet.add(nodeClass);
      }

      final Map<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> nodeConstructorsMap1 = new HashMap<>();
      final Map<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> nodeConstructorsMap2 = new HashMap<>();
      for (Class<? extends SyntaxTree.Node> nodeClass : nodeClassSet) {
        final List<? extends NodeConstructor<? extends SyntaxTree.Node>> nodeConstructors = getNodeConstructors(nodeClass);
        buildNodeConstructorMap(nodeClass, nodeConstructors, nodeConstructorsMap1, nodeConstructorsMap2);
        Class<?> superClass = nodeClass.getSuperclass();
        while (SyntaxTree.Node.class.isAssignableFrom(superClass)) {
          buildNodeConstructorMap((Class<? extends SyntaxTree.Node>) superClass, nodeConstructors, nodeConstructorsMap1, nodeConstructorsMap2);
          superClass = superClass.getSuperclass();
        }
      }

      final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap = getPrefixClassesMap(nodeConstructorsMap1.entrySet());

      final NodeAnalyseForest nodeAnalyseForest = new NodeAnalyseForest();

      for (Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> entry : nodeConstructorsMap1.entrySet()) {
        final Class<? extends SyntaxTree.Node> nodeClass = entry.getKey();
        final NodeAnalyseTree<? extends SyntaxTree.Node> nodeAnalyseTree = new NodeAnalyseTree<>(nodeClass);
        nodeAnalyseForest.trees.put(nodeClass, nodeAnalyseTree);
        buildNodeAnalyseTree1(nodeAnalyseTree, (List) entry.getValue(), prefixClassesMap);
      }

      for (Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> entry : nodeConstructorsMap2.entrySet()) {
        final Class<? extends SyntaxTree.Node> nodeClass = entry.getKey();
        final NodeAnalyseTree<? extends SyntaxTree.Node> nodeAnalyseTree1 = nodeAnalyseForest.trees.get(nodeClass);
        final NodeAnalyseTree<? extends SyntaxTree.Node> nodeAnalyseTree2;
        if (nodeAnalyseTree1 == null) {
          nodeAnalyseTree2 = new NodeAnalyseTree(nodeClass);
        } else {
          nodeAnalyseTree2 = new NodeAnalyseTree(nodeClass, nodeAnalyseTree1.root1);
        }
        nodeAnalyseForest.trees.put(nodeClass, nodeAnalyseTree2);
        buildNodeAnalyseTree2(nodeAnalyseTree2, (List) entry.getValue(), prefixClassesMap);
      }

      nodeAnalyseForest.postBuild(new HashMap<>());

      return nodeAnalyseForest;
    }

    @SuppressWarnings("unchecked")
    private static <TNode extends SyntaxTree.Node> void buildNodeAnalyseTree(
        final NodeAnalyseTree.Node<TNode> nodeAnalyseTreeNode,
        final NodeConstructor<TNode> nodeConstructor,
        final int nodeConstructorParameterIndex,
        final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap
    ) {
      if (nodeConstructorParameterIndex == nodeConstructor.getNonnullParameterCount()) {
        nodeAnalyseTreeNode.nodeConstructorList.add(nodeConstructor);
        return;
      }
      final NodeConstructorParameter nodeConstructorParameter = nodeConstructor.getNonnullParameter(nodeConstructorParameterIndex);
      final Class<? extends SyntaxTree.Node> nodeConstructorParameterClass;
      if (nodeConstructorParameter.elementClass != null) {
        nodeConstructorParameterClass = nodeConstructorParameter.elementClass;
      } else {
        nodeConstructorParameterClass = nodeConstructorParameter.clazz;
      }
      if (Lexical.class.isAssignableFrom(nodeConstructorParameterClass)) {
        buildNodeAnalyseTree(
            nodeAnalyseTreeNode,
            nodeConstructor,
            nodeConstructorParameterIndex,
            (Class<? extends Lexical>) nodeConstructorParameterClass,
            prefixClassesMap
        );
      } else {
        final Set<Class<? extends Lexical<?>>> prefixClasses = prefixClassesMap.get(nodeConstructorParameterClass);
        if (prefixClasses == null) {
          // TODO nodeConstructorParameterClass未注册到分析器中
          throw new UnsupportedOperationException();
        }
        for (Class<? extends Lexical> prefixClass : prefixClasses) {
          buildNodeAnalyseTree(
              nodeAnalyseTreeNode,
              nodeConstructor,
              nodeConstructorParameterIndex,
              prefixClass,
              prefixClassesMap
          );
        }
      }
    }

    private static <TNode extends SyntaxTree.Node> void buildNodeAnalyseTree(
        final NodeAnalyseTree.Node<TNode> nodeAnalyseTreeNode,
        final NodeConstructor<TNode> nodeConstructor,
        final int nodeConstructorParameterIndex,
        final Class<? extends Lexical> prefixClass,
        final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap
    ) {
      final NodeConstructorParameter nodeConstructorParameter = nodeConstructor.getNonnullParameter(nodeConstructorParameterIndex);
      final Class<? extends SyntaxTree.Node> nodeClass;
      final Class<? extends Lexical> delimiterClass;
      if (nodeConstructorParameter.elementClass != null) {
        nodeClass = nodeConstructorParameter.elementClass;
        delimiterClass = nodeConstructorParameter.delimiterClass;
      } else {
        nodeClass = nodeConstructorParameter.clazz;
        delimiterClass = null;
      }
      List<NodeAnalyseTree.Node<TNode>> children = nodeAnalyseTreeNode.children.get(prefixClass);
      NodeAnalyseTree.Node<TNode> child;
      if (children == null) {
        child = new NodeAnalyseTree.Node<>(nodeClass, delimiterClass);
        children = new ArrayList<>();
        children.add(child);
        nodeAnalyseTreeNode.children.put(prefixClass, children);
      } else {
        child = null;
        for (int index = 0; index < children.size(); index++) {
          if (children.get(index).nodeClass == nodeClass) {
            child = children.get(index);
            break;
          }
        }
        if (child == null) {
          child = new NodeAnalyseTree.Node<>(nodeClass, delimiterClass);
          children.add(child);
        }
      }
      buildNodeAnalyseTree(child, nodeConstructor, nodeConstructorParameterIndex + 1, prefixClassesMap);
    }

    private static <TNode extends SyntaxTree.Node> void buildNodeAnalyseTree1(
        final NodeAnalyseTree<TNode> nodeAnalyseTree,
        final List<NodeConstructor<TNode>> nodeConstructorList,
        final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap
    ) {
      for (NodeConstructor<TNode> nodeConstructor : nodeConstructorList) {
        buildNodeAnalyseTree(nodeAnalyseTree.root1, nodeConstructor, 0, prefixClassesMap);
      }
    }

    private static <TNode extends SyntaxTree.Node> void buildNodeAnalyseTree2(
        final NodeAnalyseTree<TNode> nodeAnalyseTree,
        final List<NodeConstructor<TNode>> nodeConstructorList,
        final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap
    ) {
      for (NodeConstructor<TNode> nodeConstructor : nodeConstructorList) {
        buildNodeAnalyseTree(nodeAnalyseTree.root2, nodeConstructor, 1, prefixClassesMap);
      }
    }

    private static void buildNodeConstructorMap(
        final Class<? extends SyntaxTree.Node> nodeClass,
        final List<? extends NodeConstructor<? extends SyntaxTree.Node>> nodeConstructors,
        final Map<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> nodeConstructorsMap1,
        final Map<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> nodeConstructorsMap2
    ) {
      for (NodeConstructor<?> nodeConstructor : nodeConstructors) {
        final NodeConstructorParameter firstParameter = nodeConstructor.getNonnullParameter(0);
        final Class<? extends SyntaxTree.Node> firstParameterClass;
        if (firstParameter.elementClass != null) {
          firstParameterClass = firstParameter.elementClass;
        } else {
          firstParameterClass = firstParameter.clazz;
        }
        if (nodeClass.isAssignableFrom(firstParameterClass)) {
          List<NodeConstructor<?>> nodeConstructors2 = nodeConstructorsMap2.get(nodeClass);
          if (nodeConstructors2 == null) {
            nodeConstructors2 = new ArrayList<>();
            nodeConstructorsMap2.put(nodeClass, nodeConstructors2);
          }
          nodeConstructors2.add(nodeConstructor);
        } else {
          List<NodeConstructor<?>> nodeConstructors1 = nodeConstructorsMap1.get(nodeClass);
          if (nodeConstructors1 == null) {
            nodeConstructors1 = new ArrayList<>();
            nodeConstructorsMap1.put(nodeClass, nodeConstructors1);
          }
          nodeConstructors1.add(nodeConstructor);
        }
      }
    }

    private static Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> getPrefixClassesMap(
        final Set<Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>>> entrySet
    ) {
      final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap = new HashMap<>();
      final List<Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>>> unreadyList = new ArrayList<>();
      final List<Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>>> entryList = new ArrayList<>(entrySet);
      while (true) {
        for (Map.Entry<Class<? extends SyntaxTree.Node>, List<NodeConstructor<?>>> entry : entryList) {
          final List<NodeConstructor<?>> nodeConstructors = entry.getValue();
          final Set<Class<? extends Lexical<?>>> prefixClasses = new HashSet<>();
          boolean allDependenciesAreReady = true;
          for (NodeConstructor<?> nodeConstructor : nodeConstructors) {
            final NodeConstructorParameter firstParameter = nodeConstructor.getNonnullParameter(0);
            final Class<? extends SyntaxTree.Node> firstParameterClass;
            if (firstParameter.elementClass != null) {
              firstParameterClass = firstParameter.elementClass;
            } else {
              firstParameterClass = firstParameter.clazz;
            }
            if (!getPrefixClassesMap(firstParameterClass, prefixClasses, prefixClassesMap)) {
              allDependenciesAreReady = false;
              break;
            }
          }
          if (allDependenciesAreReady) {
            prefixClassesMap.put(entry.getKey(), prefixClasses);
          } else {
            unreadyList.add(entry);
          }
        }
        if (unreadyList.isEmpty()) {
          break;
        } else {
          if (entryList.size() == unreadyList.size()) {
            // TODO
            throw new UnsupportedOperationException();
          }
          entryList.clear();
          entryList.addAll(unreadyList);
          unreadyList.clear();
        }
      }
      return prefixClassesMap;
    }

    @SuppressWarnings("unchecked")
    private static boolean getPrefixClassesMap(
        final Class<? extends SyntaxTree.Node> prefixClass,
        final Set<Class<? extends Lexical<?>>> prefixClasses,
        final Map<Class<? extends SyntaxTree.Node>, Set<Class<? extends Lexical<?>>>> prefixClassesMap
    ) {
      if (Lexical.class.isAssignableFrom(prefixClass)) {
        prefixClasses.add((Class<? extends Lexical<?>>) prefixClass);
        return true;
      } else {
        if (prefixClassesMap.containsKey(prefixClass)) {
          prefixClasses.addAll(prefixClassesMap.get(prefixClass));
          return true;
        } else {
          return false;
        }
      }
    }

    @SuppressWarnings("unchecked")
    private static <TNode extends SyntaxTree.Node> List<NodeConstructor<TNode>> getNodeConstructors(final Class<TNode> nodeClass) {
      final List<NodeConstructor<TNode>> nodeConstructorList = new ArrayList<>();
      for (java.lang.reflect.Constructor nodeConstructor : nodeClass.getConstructors()) {
        final String errorMassage = checkNodeConstructor(nodeConstructor);
        if (errorMassage != null) {
          LOGGER.warn("Constructor " + nodeConstructor.toString() + " is ignored. " + errorMassage);
          continue;
        }
        getNodeConstructors(
            nodeConstructor,
            0,
            new NodeConstructorParameter[nodeConstructor.getParameterCount()],
            nodeConstructorList
        );
      }
      // 检查参数
      for (NodeConstructor<TNode> nodeConstructor : nodeConstructorList) {
        final int parameterCount = nodeConstructor.getNonnullParameterCount();
        for (int parameterIndex = 0; parameterIndex < parameterCount; parameterIndex++) {
          final NodeConstructorParameter parameter = nodeConstructor.getNonnullParameter(parameterIndex);
          if (parameter.delimiterClass != null
              && parameter.delimiterClass == Lexical.Whitespace.class
              && parameterIndex < parameterCount - 1) {
            // 这种情况下，当前列表参数的后面的一个参数类型，最好不要和列表参数的元素类型相同
            final NodeConstructorParameter nextParameter = nodeConstructor.getNonnullParameter(parameterIndex + 1);
            final Class<?> nextParameterClass;
            if (nextParameter.elementClass != null) {
              nextParameterClass = nextParameter.elementClass;
            } else {
              nextParameterClass = nextParameter.clazz;
            }
            if (parameter.elementClass.isAssignableFrom(nextParameterClass)) {
              LOGGER.warn("// TODO");// TODO
            }
          }
        }
      }
      return nodeConstructorList;
    }

    @SuppressWarnings("unchecked")
    private static <TNode extends SyntaxTree.Node> void getNodeConstructors(
        final java.lang.reflect.Constructor nodeConstructor,
        final int nodeConstructorParameterIndex,
        final NodeConstructorParameter[] nodeConstructorParameters,
        final List<NodeConstructor<TNode>> nodeConstructorList
    ) {
      final Class nodeConstructorParameterClass = nodeConstructor.getParameterTypes()[nodeConstructorParameterIndex];
      final NodeConstructorParameter nodeConstructorParameter;
      if (SyntaxTree.NodeList.class == nodeConstructor.getParameterTypes()[nodeConstructorParameterIndex]) {
        final ParameterizedType nodeConstructorParameterType = (ParameterizedType) nodeConstructor.getGenericParameterTypes()[nodeConstructorParameterIndex];
        nodeConstructorParameter = NodeConstructorParameter.forNodeList(
            nodeConstructorParameterClass,
            (Class<? extends SyntaxTree.Node>) nodeConstructorParameterType.getActualTypeArguments()[0],
            (Class<? extends Lexical>) nodeConstructorParameterType.getActualTypeArguments()[1]
        );
      } else {
        nodeConstructorParameter = NodeConstructorParameter.forNode(nodeConstructorParameterClass);
      }
      nodeConstructorParameters[nodeConstructorParameterIndex] = nodeConstructorParameter;
      if (nodeConstructorParameterIndex == nodeConstructor.getParameterCount() - 1) {
        // the last parameter.
        nodeConstructorList.add(NodeConstructor.of(nodeConstructor, nodeConstructorParameters.clone()));
        if (getNodeConstructorParameterOptionalAnnotation(nodeConstructor, nodeConstructorParameterIndex) != null) {
          nodeConstructorParameters[nodeConstructorParameterIndex] = NodeConstructorParameter.forNull();
          nodeConstructorList.add(NodeConstructor.of(nodeConstructor, nodeConstructorParameters.clone()));
        }
      } else {
        getNodeConstructors(nodeConstructor, nodeConstructorParameterIndex + 1, nodeConstructorParameters, nodeConstructorList);
        if (getNodeConstructorParameterOptionalAnnotation(nodeConstructor, nodeConstructorParameterIndex) != null) {
          nodeConstructorParameters[nodeConstructorParameterIndex] = NodeConstructorParameter.forNull();
          getNodeConstructors(nodeConstructor, nodeConstructorParameterIndex + 1, nodeConstructorParameters, nodeConstructorList);
        }
      }
    }

    private static Annotation getNodeConstructorParameterOptionalAnnotation(
        final java.lang.reflect.Constructor nodeConstructor,
        final int nodeConstructorParameterIndex
    ) {
      final Annotation[] nodeConstructorParameterAnnotations = nodeConstructor.getParameterAnnotations()[nodeConstructorParameterIndex];
      for (Annotation nodeConstructionParameterAnnotation : nodeConstructorParameterAnnotations) {
        if (com.huawei.cloudtable.leo.language.annotation.Optional.class.isAssignableFrom(nodeConstructionParameterAnnotation.annotationType())) {
          return nodeConstructionParameterAnnotation;
        }
      }
      return null;
    }

    private static String checkNodeClass(final Class<? extends SyntaxTree.Node> nodeClass) {
      if (Modifier.isAbstract(nodeClass.getModifiers())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      if (!Modifier.isPublic(nodeClass.getModifiers())) {
        // TODO
        throw new UnsupportedOperationException();
      }
      return null;
    }

    private static String checkNodeConstructor(final java.lang.reflect.Constructor nodeConstructor) {
      if (!Modifier.isPublic(nodeConstructor.getModifiers())) {
        return "It is not a public constructor.";
      }
      if (nodeConstructor.getParameterCount() == 0) {
        // TODO 除lexical外，其它不允许无参
        throw new UnsupportedOperationException();
      }
      boolean hasRequiredParameter = false;
      for (int index = 0; index < nodeConstructor.getParameterCount(); index++) {
        if (getNodeConstructorParameterOptionalAnnotation(nodeConstructor, index) == null) {
          hasRequiredParameter = true;
          break;
        }
      }
      if (!hasRequiredParameter) {
        // TODO 除lexical外，其它不允许无参
        throw new UnsupportedOperationException();
      }
      for (int index = 0; index < nodeConstructor.getParameterCount(); index++) {
        final Class<?> nodeConstructorParameterClass = nodeConstructor.getParameterTypes()[index];
        if (!SyntaxTree.Node.class.isAssignableFrom(nodeConstructorParameterClass)) {
          // TODO return error message.
          throw new UnsupportedOperationException();
        }
        if (Lexical.Symbol.class == nodeConstructorParameterClass) {
          // TODO return error message.
          throw new UnsupportedOperationException();
        }
        if (Lexical.Keyword.class == nodeConstructorParameterClass) {
          // TODO return error message.
          throw new UnsupportedOperationException();
        }
        if (SyntaxTree.NodeList.class == nodeConstructorParameterClass) {
          if (nodeConstructor.getGenericParameterTypes()[index] instanceof ParameterizedType) {
            final ParameterizedType nodeConstructorParameterType = (ParameterizedType) nodeConstructor.getGenericParameterTypes()[index];
            final Type elementClass = nodeConstructorParameterType.getActualTypeArguments()[0];
            if (elementClass.getClass() != Class.class) {
              // TODO 指定的ElementClass不是简单的Class 抛异常
              throw new UnsupportedOperationException();
            }
            if (elementClass == SyntaxTree.Node.class) {
              // TODO 暂不支持 抛异常
              throw new UnsupportedOperationException();
            }
            final Type delimiterClass = nodeConstructorParameterType.getActualTypeArguments()[1];
            if (delimiterClass.getClass() != Class.class) {
              // TODO 指定的DelimiterClass不是简单的Class 抛异常
              throw new UnsupportedOperationException();
            }
          } else {
            // TODO 未指定ElementClass和DelimiterClass 抛异常
            throw new UnsupportedOperationException();
          }
        }
      }
      return null;
    }

  }

  private SyntaxAnalyzer(final LexicalAnalyzer analyzer, final NodeAnalyseForest nodeAnalyseForest) {
    this.analyzer = analyzer;
    this.analyseForest = nodeAnalyseForest;
  }

  private final LexicalAnalyzer analyzer;

  private final NodeAnalyseForest analyseForest;

  @SuppressWarnings("unchecked")
  public <TNode extends SyntaxTree.Node> SyntaxTree<TNode> analyse(final String string, final Class<TNode> nodeClass) throws SyntaxException {
    if (string == null) {
      throw new IllegalArgumentException("Argument [string] is null.");
    }
    final NodeAnalyseTree<TNode> nodeAnalyseTree = (NodeAnalyseTree<TNode>) this.analyseForest.trees.get(nodeClass);
    if (nodeAnalyseTree == null) {
      // TODO 抛异常
      throw new UnsupportedOperationException();
    }
    return new SyntaxTree<>(nodeAnalyseTree.analyse(this.analyseForest, this.analyzer.analyse(string)));
  }

  private enum ErrorType {

    EOF,

    AMBIGUOUS

  }

  private static final class ErrorLexical extends Lexical {

    ErrorLexical(final ErrorType type, final Lexical startLexical, final Lexical endLexical) {
      this.type = type;
      this.startLexical = startLexical;
      this.endLexical = endLexical;
    }

    final ErrorType type;

    final Lexical startLexical;

    final Lexical endLexical;

    @Override
    public Object getValue() {
      throw new UnsupportedOperationException();
    }

    void setPosition(final LexicalIterator lexicalIterator) {
      if (this.getPosition() == null) {
        this.setPosition(
            SyntaxTree.NodePosition.of(
                this.startLexical == null ? lexicalIterator.getString().length() : this.startLexical.getPosition().getStart(),
                this.endLexical == null ? lexicalIterator.getString().length() : this.endLexical.getPosition().getEnd()
            )
        );
      }
    }

    SyntaxException toException(final LexicalIterator lexicalIterator) {
      this.setPosition(lexicalIterator);
      final SyntaxTree.NodePosition position = this.getPosition();
      switch (this.type) {
        case EOF:
          return new SyntaxException(position, "EOF");
        case AMBIGUOUS:
          return new SyntaxException(position, "Ambiguous");
        default:
          throw new RuntimeException("Unsupported error type. " + this.type.name());
      }
    }

  }

  private static final class NodeAnalyseForest {

    final Map<Class<? extends SyntaxTree.Node>, NodeAnalyseTree<?>> trees = new HashMap<>();

    void postBuild(final Map<Class<? extends Lexical>, NodeConstructor<? extends Lexical>> lexicalConstructorMap) {
      for (NodeAnalyseTree<?> tree : this.trees.values()) {
        tree.postBuild(this, lexicalConstructorMap);
      }
    }

  }

  private static final class NodeAnalyseTree<TNode extends SyntaxTree.Node> {

    static final ThreadLocal<NodeAnalyseStack> NODE_ANALYSE_STACK_CACHE = new ThreadLocal<NodeAnalyseStack>() {
      @Override
      protected NodeAnalyseStack initialValue() {
        return new NodeAnalyseStack();
      }
    };

    static final ThreadLocal<NodeConstructStack> NODE_CONSTRUCT_STACK_CACHE = new ThreadLocal<NodeConstructStack>() {
      @Override
      protected NodeConstructStack initialValue() {
        return new NodeConstructStack();
      }
    };

    NodeAnalyseTree(final Class<TNode> nodeClass) {
      this.nodeClass = nodeClass;
      this.root1 = new Node<>(nodeClass, null);
      this.root2 = new Node<>(nodeClass, null);
      this.analyser = new Analyser<TNode>() {
        @Override
        Lexical<?> analyse(
            final NodeAnalyseForest nodeAnalyseForest,
            final NodeAnalyseStack nodeAnalyseStack,
            final Lexical<?> currentLexical,
            final List<Node<TNode>> children,
            final LexicalIterator lexicalIterator
        ) {
          return NodeAnalyseTree.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, children, lexicalIterator);
        }
      };
    }

    NodeAnalyseTree(final Class<TNode> nodeClass, final Node<TNode> root1) {
      final Node<TNode> root2 = new Node<>(nodeClass, null);
      this.nodeClass = nodeClass;
      this.root1 = root1;
      this.root2 = root2;
      this.analyser = new Analyser<TNode>() {
        @Override
        Lexical<?> analyse(
            final NodeAnalyseForest nodeAnalyseForest,
            final NodeAnalyseStack nodeAnalyseStack,
            final Lexical<?> currentLexical,
            final List<Node<TNode>> children,
            final LexicalIterator lexicalIterator
        ) {
          final Lexical<?> nextLexical = NodeAnalyseTree.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, children, lexicalIterator);
          if (nextLexical != null && !(nextLexical instanceof ErrorLexical)) {
            final List<Node<TNode>> children2 = root2.children.get(nextLexical.getClass());
            if (children2 != null) {
              final Lexical<?> innerNextLexical = this.analyse(nodeAnalyseForest, nodeAnalyseStack, nextLexical, children2, lexicalIterator);
              if (!(innerNextLexical instanceof ErrorLexical)) {
                return innerNextLexical;
              }
            }
          }
          return nextLexical;
        }
      };
    }

    final Class<TNode> nodeClass;

    final Node<TNode> root1;

    final Node<TNode> root2;// Start with self.

    private final Analyser<TNode> analyser;

    TNode analyse(final NodeAnalyseForest nodeAnalyseForest, final LexicalIterator lexicalIterator) throws SyntaxException {
      final Lexical<?> firstLexical = lexicalIterator.next();
      if (firstLexical == null) {
        // 空串
        return null;
      }
      final List<Node<TNode>> children = this.root1.children.get(firstLexical.getClass());
      if (children == null) {
        // TODO 以非法类型的词开头，抛语法错误
        throw new SyntaxException(firstLexical.getPosition());
      }
      final NodeAnalyseStack nodeAnalyseStack = NODE_ANALYSE_STACK_CACHE.get();
      try {
        final Lexical<?> nextLexical = this.analyser.analyse(nodeAnalyseForest, nodeAnalyseStack, firstLexical, children, lexicalIterator);
        if (nextLexical != null) {
          if (nextLexical instanceof ErrorLexical) {
            ErrorLexical errorLexical = (ErrorLexical) nextLexical;
            if (errorLexical.type == ErrorType.AMBIGUOUS && errorLexical.getPosition() == null) {
              errorLexical = buildAmbiguous(firstLexical, errorLexical);
              errorLexical.setPosition(lexicalIterator);
            }
            throw errorLexical.toException(lexicalIterator);
          } else {
            throw new SyntaxException(nextLexical.getPosition());
          }
        }
        // 将分析栈转成语法树
        final NodeConstructor<?> rootConstructor = nodeAnalyseStack.pop();
        final NodeConstructStack rootConstructStack = NODE_CONSTRUCT_STACK_CACHE.get();
        try {
          lexicalIterator.toTail();
          rootConstructor.construct(nodeAnalyseStack, lexicalIterator, rootConstructStack);
          final TNode root = this.nodeClass.cast(rootConstructStack.pop());
          if (!rootConstructStack.isEmpty()) {
            throw new RuntimeException();
          }
          return root;
        } finally {
          rootConstructStack.clear();
        }
      } finally {
        nodeAnalyseStack.clear();
      }
    }

    Lexical analyse(
        final NodeAnalyseForest nodeAnalyseForest,
        final NodeAnalyseStack nodeAnalyseStack,
        final Lexical<?> currentLexical,
        final LexicalIterator lexicalIterator
    ) {
      Lexical<?> nextLexical = this.analyser.analyse(
          nodeAnalyseForest,
          nodeAnalyseStack,
          currentLexical,
          this.root1.children.get(currentLexical.getClass()),
          lexicalIterator
      );
      if (nextLexical instanceof ErrorLexical) {
        ErrorLexical errorLexical = (ErrorLexical) nextLexical;
        if (errorLexical.type == ErrorType.AMBIGUOUS) {
          errorLexical = buildAmbiguous(currentLexical, errorLexical);
          errorLexical.setPosition(lexicalIterator);
          nextLexical = errorLexical;
        }
      }
      return nextLexical;
    }

    Lexical analyseTentatively(
        final NodeAnalyseForest nodeAnalyseForest,
        final NodeAnalyseStack nodeAnalyseStack,
        final Lexical<?> currentLexical,
        final LexicalIterator lexicalIterator
    ) {
      final List<Node<TNode>> children = this.root1.children.get(currentLexical.getClass());
      if (children == null) {
        return currentLexical;
      } else {
        final int analyseStackInitializeSize = nodeAnalyseStack.size();
        Lexical<?> nextLexical = this.analyser.analyse(
            nodeAnalyseForest,
            nodeAnalyseStack,
            currentLexical,
            children,
            lexicalIterator
        );
        if (nextLexical instanceof ErrorLexical) {
          ErrorLexical errorLexical = (ErrorLexical) nextLexical;
          if (errorLexical.type == ErrorType.AMBIGUOUS) {
            errorLexical = buildAmbiguous(currentLexical, errorLexical);
            errorLexical.setPosition(lexicalIterator);
            nextLexical = errorLexical;
          } else {
            // 还原分析栈
            nodeAnalyseStack.clear(analyseStackInitializeSize);
            nextLexical = currentLexical;
          }
        }
        return nextLexical;
      }
    }

    static <TNode extends SyntaxTree.Node> Lexical<?> analyse(
        final NodeAnalyseForest nodeAnalyseForest,
        final NodeAnalyseStack nodeAnalyseStack,
        final Lexical<?> currentLexical,
        final List<Node<TNode>> children,
        final LexicalIterator lexicalIterator
    ) {
      if (children.size() == 1) {
        return children.get(0).analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
      } else {
        Integer maximumMatchedDepth = null;
        Lexical<?> maximumMatchedNextLexical = null;
        ErrorLexical errorLexical = null;
        boolean isAmbiguous = false;
        final int analyseStackInitializeSize = nodeAnalyseStack.size();
        for (int index = 0; index < children.size(); index++) {
          final NodeAnalyseTree.Node<TNode> child = children.get(index);
          final int analyseStackSize = nodeAnalyseStack.size();
          final Lexical<?> nextLexical = child.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
          if (nextLexical instanceof ErrorLexical) {
            final ErrorLexical innerErrorLexical = (ErrorLexical) nextLexical;
            if (innerErrorLexical.type == ErrorType.AMBIGUOUS) {
              final int matchedDepth;
              if (innerErrorLexical.endLexical == null) {
                matchedDepth = Integer.MAX_VALUE;
              } else {
                matchedDepth = innerErrorLexical.endLexical.getIndex() - currentLexical.getIndex();
              }
              if (maximumMatchedDepth == null) {
                maximumMatchedDepth = matchedDepth;
                maximumMatchedNextLexical = innerErrorLexical.endLexical;
              } else {
                if (matchedDepth < maximumMatchedDepth) {
                  nodeAnalyseStack.clear(analyseStackSize);
                  continue;
                }
                if (matchedDepth == maximumMatchedDepth) {
                  nodeAnalyseStack.clear(analyseStackSize);
                } else {
                  maximumMatchedDepth = matchedDepth;
                  maximumMatchedNextLexical = nextLexical;
                  nodeAnalyseStack.clear(analyseStackInitializeSize, analyseStackSize);
                }
              }
              isAmbiguous = true;
            } else {
              if (errorLexical == null) {
                errorLexical = innerErrorLexical;
              }
              nodeAnalyseStack.clear(analyseStackSize);
            }
          } else {
            final int matchedDepth;
            if (nextLexical == null) {
              matchedDepth = Integer.MAX_VALUE;
            } else {
              matchedDepth = nextLexical.getIndex() - currentLexical.getIndex();
            }
            if (maximumMatchedDepth == null) {
              maximumMatchedDepth = matchedDepth;
              maximumMatchedNextLexical = nextLexical;
            } else {
              if (matchedDepth < maximumMatchedDepth) {
                nodeAnalyseStack.clear(analyseStackSize);
                continue;
              }
              if (matchedDepth == maximumMatchedDepth) {
                nodeAnalyseStack.clear(analyseStackSize);
                isAmbiguous = true;
                continue;
              }
              maximumMatchedDepth = matchedDepth;
              maximumMatchedNextLexical = nextLexical;
              isAmbiguous = false;
              nodeAnalyseStack.clear(analyseStackInitializeSize, analyseStackSize);
            }
          }
        }
        if (maximumMatchedDepth != null) {
          if (isAmbiguous) {
            // 有岐义
            nodeAnalyseStack.clear(analyseStackInitializeSize);
            return new ErrorLexical(ErrorType.AMBIGUOUS, currentLexical, maximumMatchedNextLexical);
          }
          return maximumMatchedNextLexical;
        }
        return errorLexical;
      }
    }

    void postBuild(
        final NodeAnalyseForest nodeAnalyseForest,
        final Map<Class<? extends Lexical>, NodeConstructor<? extends Lexical>> lexicalConstructorMap
    ) {
      this.root1.postBuild(nodeAnalyseForest, lexicalConstructorMap);
      this.root2.postBuild(nodeAnalyseForest, lexicalConstructorMap);
    }

    private static NodeConstructor<? extends Lexical> getOrCreateLexicalConstructor(
        final Class<? extends Lexical> lexicalClass,
        final Map<Class<? extends Lexical>, NodeConstructor<? extends Lexical>> lexicalConstructorMap
    ) {
      NodeConstructor<? extends Lexical> lexicalConstructor = lexicalConstructorMap.get(lexicalClass);
      if (lexicalConstructor == null) {
        lexicalConstructor = NodeConstructor.of(lexicalClass);
        lexicalConstructorMap.put(lexicalClass, lexicalConstructor);
      }
      return lexicalConstructor;
    }

    private static ErrorLexical buildAmbiguous(final Lexical<?> currentLexical, final ErrorLexical errorLexical) {
      return new ErrorLexical(ErrorType.AMBIGUOUS, currentLexical, errorLexical.endLexical);
    }

    private static ErrorLexical buildEOF(final Lexical<?> nextLexical) {
      return new ErrorLexical(ErrorType.EOF, nextLexical, null);
    }

    @Override
    public final String toString() {
      return this.root1.nodeClass.getSimpleName();
    }

    private static abstract class Analyser<TNode extends SyntaxTree.Node> {

      abstract Lexical<?> analyse(
          NodeAnalyseForest nodeAnalyseForest,
          NodeAnalyseStack nodeAnalyseStack,
          Lexical<?> currentLexical,
          List<Node<TNode>> children,
          LexicalIterator lexicalIterator
      );

    }

    static final class Node<TNode extends SyntaxTree.Node> {

      Node(final Class<? extends SyntaxTree.Node> nodeClass, final Class<? extends Lexical> delimiterClass) {
        this.nodeClass = nodeClass;
        this.delimiterClass = delimiterClass;
      }

      private NodeAnalyzer nodeAnalyzer;

      final Class<? extends SyntaxTree.Node> nodeClass;

      final List<NodeConstructor<TNode>> nodeConstructorList = new ArrayList<>(1);

      private NodeConstructor<? extends Lexical> delimiterConstructor;

      final Class<? extends Lexical> delimiterClass;

      final Map<Class<? extends Lexical>, List<Node<TNode>>> children = new HashMap<>();

      /**
       * @return 返回下一个Lexical
       */
      Lexical analyse(
          final NodeAnalyseForest nodeAnalyseForest,
          final NodeAnalyseStack nodeAnalyseStack,
          final Lexical<?> currentLexical,
          final LexicalIterator lexicalIterator
      ) {
        final int analyseStackInitializeSize = nodeAnalyseStack.size();
        Lexical<?> nextLexical = this.nodeAnalyzer.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
        if (nextLexical instanceof ErrorLexical) {
          return nextLexical;
        }
        if (this.delimiterClass != null && nextLexical != null) {
          if (this.delimiterClass == Lexical.Whitespace.class) {
            do {
              final Lexical<?> originalNextLexical = nextLexical;
              // 尝试解析下一个元素
              nextLexical = this.nodeAnalyzer.analyseTentatively(nodeAnalyseForest, nodeAnalyseStack, nextLexical, lexicalIterator);
              if (nextLexical == originalNextLexical) {
                break;
              }
              if (nextLexical instanceof ErrorLexical) {
                // 只可能是有歧义的情况
                if (((ErrorLexical) nextLexical).type != ErrorType.AMBIGUOUS) {
                  throw new RuntimeException(((ErrorLexical) nextLexical).type.name());
                }
                return nextLexical;
              }
            } while (nextLexical != null);
          } else {
            while (nextLexical.getClass() == this.delimiterClass) {
              nodeAnalyseStack.push(this.delimiterConstructor);
              nextLexical = lexicalIterator.next(nextLexical);
              if (nextLexical == null) {
                nodeAnalyseStack.clear(analyseStackInitializeSize);
                return buildEOF(null);
              }
              nextLexical = this.nodeAnalyzer.analyse(nodeAnalyseForest, nodeAnalyseStack, nextLexical, lexicalIterator);
              if (nextLexical == null) {
                break;
              }
              if (nextLexical instanceof ErrorLexical) {
                return nextLexical;
              }
            }
          }
        }
        ErrorLexical errorLexical = null;
        if (nextLexical != null) {
          final List<Node<TNode>> children = this.children.get(nextLexical.getClass());
          if (children != null) {
            final Lexical<?> innerNextLexical = NodeAnalyseTree.analyse(nodeAnalyseForest, nodeAnalyseStack, nextLexical, children, lexicalIterator);
            if (innerNextLexical instanceof ErrorLexical) {
              errorLexical = (ErrorLexical) innerNextLexical;
              if (errorLexical.type == ErrorType.AMBIGUOUS) {
                return innerNextLexical;
              }
            } else {
              return innerNextLexical;
            }
          }
        }
        switch (this.nodeConstructorList.size()) {
          case 0:
            nodeAnalyseStack.clear(analyseStackInitializeSize);
            if (errorLexical != null) {
              return errorLexical;
            } else {
              return buildEOF(nextLexical);
            }
          case 1:
            nodeAnalyseStack.push(this.nodeConstructorList.get(0));
            return nextLexical;
          default:
            nodeAnalyseStack.clear(analyseStackInitializeSize);
            return new ErrorLexical(ErrorType.AMBIGUOUS, currentLexical, currentLexical);
        }
      }

      void postBuild(
          final NodeAnalyseForest nodeAnalyseForest,
          final Map<Class<? extends Lexical>, NodeConstructor<? extends Lexical>> lexicalConstructorMap
      ) {
        this.nodeAnalyzer = NodeAnalyzer.of(this.nodeClass, nodeAnalyseForest, lexicalConstructorMap);
        if (this.delimiterClass != null) {
          this.delimiterConstructor = getOrCreateLexicalConstructor(this.delimiterClass, lexicalConstructorMap);
        }
        for (List<Node<TNode>> children : this.children.values()) {
          for (Node<TNode> child : children) {
            child.postBuild(nodeAnalyseForest, lexicalConstructorMap);
          }
        }
      }

    }

    static abstract class NodeAnalyzer {

      @SuppressWarnings("unchecked")
      static NodeAnalyzer of(
          final Class<? extends SyntaxTree.Node> nodeClass,
          final NodeAnalyseForest nodeAnalyseForest,
          final Map<Class<? extends Lexical>, NodeConstructor<? extends Lexical>> lexicalConstructorMap
      ) {
        if (Lexical.class.isAssignableFrom(nodeClass)) {
          final Class<? extends Lexical> lexicalClass = (Class<? extends Lexical>) nodeClass;
          final NodeConstructor<?> lexicalConstructor = getOrCreateLexicalConstructor(lexicalClass, lexicalConstructorMap);
          return new NodeAnalyzer() {
            @Override
            Lexical analyse(
                final NodeAnalyseForest nodeAnalyseForest,
                final NodeAnalyseStack nodeAnalyseStack,
                final Lexical<?> currentLexical,
                final LexicalIterator lexicalIterator
            ) {
              nodeAnalyseStack.push(lexicalConstructor);
              return lexicalIterator.next(currentLexical);// 消耗掉一个Lexical
            }

            @Override
            Lexical analyseTentatively(
                final NodeAnalyseForest nodeAnalyseForest,
                final NodeAnalyseStack nodeAnalyseStack,
                final Lexical<?> currentLexical,
                final LexicalIterator lexicalIterator
            ) {
              if (lexicalClass.isAssignableFrom(currentLexical.getClass())) {
                return this.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
              } else {
                return currentLexical;
              }
            }
          };
        } else {
          final NodeAnalyseTree nodeAnalyseTree = nodeAnalyseForest.trees.get(nodeClass);
          return new NodeAnalyzer() {
            @Override
            Lexical analyse(
                final NodeAnalyseForest nodeAnalyseForest,
                final NodeAnalyseStack nodeAnalyseStack,
                final Lexical<?> currentLexical,
                final LexicalIterator lexicalIterator
            ) {
              return nodeAnalyseTree.analyse(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
            }

            @Override
            Lexical analyseTentatively(
                final NodeAnalyseForest nodeAnalyseForest,
                final NodeAnalyseStack nodeAnalyseStack,
                final Lexical<?> currentLexical,
                final LexicalIterator lexicalIterator
            ) {
              return nodeAnalyseTree.analyseTentatively(nodeAnalyseForest, nodeAnalyseStack, currentLexical, lexicalIterator);
            }
          };
        }
      }

      abstract Lexical analyse(
          NodeAnalyseForest nodeAnalyseForest,
          NodeAnalyseStack nodeAnalyseStack,
          Lexical<?> currentLexical,
          LexicalIterator lexicalIterator
      );

      /**
       * 尝试性地分析，分析失败不报错（有岐义的情况除外）。
       */
      abstract Lexical analyseTentatively(
          NodeAnalyseForest nodeAnalyseForest,
          NodeAnalyseStack nodeAnalyseStack,
          Lexical<?> currentLexical,
          LexicalIterator lexicalIterator
      );

    }

  }

  private static final class NodeAnalyseStack {

    private static final int INITIALIZE_CAPACITY = 10;

    private NodeConstructor<?>[] list = new NodeConstructor<?>[INITIALIZE_CAPACITY];

    private int size = 0;

    void push(final NodeConstructor<?> nodeConstructor) {
      if (this.size == this.list.length) {
        this.growCapacity();
      }
      this.list[this.size] = nodeConstructor;
      this.size++;
    }

    NodeConstructor<?> top() {
      if (this.size == 0) {
        throw new EmptyStackException();
      }
      return this.list[this.size - 1];
    }

    NodeConstructor<?> pop() {
      if (this.size == 0) {
        throw new EmptyStackException();
      }
      final int topIndex = this.size - 1;
      final NodeConstructor<?> nodeConstructor = this.list[topIndex];
      this.list[topIndex] = null;
      this.size--;
      return nodeConstructor;
    }

    int size() {
      return this.size;
    }

    void clear() {
      Arrays.fill(this.list, 0, this.size, null);
      this.size = 0;
    }

    void clear(final int startIndex) {
      Arrays.fill(this.list, startIndex, this.size, null);
      this.size = startIndex;
    }

    void clear(final int startIndex, final int endIndex) {
      if (endIndex == this.size) {
        this.clear();
      } else {
        System.arraycopy(this.list, endIndex, this.list, endIndex - startIndex, this.size - endIndex);
        final int newSize = this.size - (endIndex - startIndex);
        Arrays.fill(this.list, newSize, this.size, null);
        this.size = newSize;
      }
    }

    private void growCapacity() {
      final NodeConstructor<?>[] list = new NodeConstructor<?>[this.list.length + INITIALIZE_CAPACITY];
      System.arraycopy(this.list, 0, list, 0, this.list.length);
      this.list = list;
    }

  }

  private static abstract class NodeConstructor<TNode extends SyntaxTree.Node> {

    private static final int NODE_PARAMETERS_TEMPLATE_CACHE_SIZE = 10;

    private static final ThreadLocal<Map<Integer, SyntaxTree.Node[]>> NODE_PARAMETERS_TEMPLATE_CACHE = new ThreadLocal<Map<Integer, SyntaxTree.Node[]>>() {
      @Override
      protected Map<Integer, SyntaxTree.Node[]> initialValue() {
        return new HashMap<>();
      }
    };

    private static SyntaxTree.Node[] getNodeParametersTemplate(final int parameterCount) {
      if (parameterCount > NODE_PARAMETERS_TEMPLATE_CACHE_SIZE) {
        return new SyntaxTree.Node[parameterCount];
      } else {
        final Map<Integer, SyntaxTree.Node[]> cache = NODE_PARAMETERS_TEMPLATE_CACHE.get();
        SyntaxTree.Node[] template = cache.get(parameterCount);
        if (template == null) {
          template = new SyntaxTree.Node[parameterCount];
          cache.put(parameterCount, template);
        }
        return template;
      }
    }

    static <TNode extends Lexical> NodeConstructor<TNode> of(final Class<TNode> lexicalClass) {
      return new NodeConstructor<TNode>(lexicalClass, new NodeConstructorParameter[0]) {
        @Override
        void construct(
            final NodeAnalyseStack nodeAnalyseStack,
            final LexicalIterator lexicalIterator,
            final NodeConstructStack nodeConstructStack
        ) {
          nodeConstructStack.push(lexicalClass.cast(lexicalIterator.prev()));
        }
      };
    }

    static <TNode extends SyntaxTree.Node> NodeConstructor<TNode> of(
        final java.lang.reflect.Constructor<TNode> nodeConstructor,
        final NodeConstructorParameter[] nodeConstructorParameters
    ) {
      return new NodeConstructor<TNode>(nodeConstructor.getDeclaringClass(), nodeConstructorParameters) {
        @Override
        void construct(
            final NodeAnalyseStack nodeAnalyseStack,
            final LexicalIterator lexicalIterator,
            final NodeConstructStack nodeConstructStack
        ) {
          for (int index = nodeConstructorParameters.length - 1; index >= 0; index--) {
            nodeConstructorParameters[index].construct(nodeAnalyseStack, lexicalIterator, nodeConstructStack);
          }
          final SyntaxTree.Node[] nodeParameters = NodeConstructor.getNodeParametersTemplate(nodeConstructorParameters.length);
          try {
            nodeConstructStack.pop(nodeParameters);
            final SyntaxTree.Node node;
            try {
              node = nodeConstructor.newInstance((Object[]) nodeParameters);
            } catch (Throwable exception) {
              throw new RuntimeException(exception);
            }
            node.setPosition(this.positionHandler.handle(nodeParameters));
            nodeConstructStack.push(node);
          } finally {
            Arrays.fill(nodeParameters, null);// Clear parameters template.
          }
        }
      };
    }

    private static int getTheFirstNonnullParameterIndex(final NodeConstructorParameter[] parameters) {
      for (int parameterIndex = 0; parameterIndex < parameters.length; parameterIndex++) {
        final NodeConstructorParameter parameter = parameters[parameterIndex];
        if (!parameter.isNull()) {
          return parameterIndex;
        }
      }
      return -1;
    }

    private static int getTheLastNonnullParameterIndex(final NodeConstructorParameter[] parameters) {
      for (int parameterIndex = parameters.length - 1; parameterIndex >= 0; parameterIndex--) {
        final NodeConstructorParameter parameter = parameters[parameterIndex];
        if (!parameter.isNull()) {
          return parameterIndex;
        }
      }
      return -1;
    }

    private NodeConstructor(final Class<TNode> clazz, final NodeConstructorParameter[] parameters) {
      final List<NodeConstructorParameter> nonnullParameters = new ArrayList<>(parameters.length);
      for (NodeConstructorParameter parameter : parameters) {
        if (!parameter.isNull()) {
          nonnullParameters.add(parameter);
        }
      }
      final int theFirstNonnullParameterIndex = getTheFirstNonnullParameterIndex(parameters);
      final int theLastNonnullParameterIndex = getTheLastNonnullParameterIndex(parameters);
      final PositionHandler positionHandler;
      if (theFirstNonnullParameterIndex == theLastNonnullParameterIndex) {
        positionHandler = new PositionHandler() {
          @Override
          SyntaxTree.NodePosition handle(final SyntaxTree.Node[] nodeParameters) {
            return nodeParameters[theFirstNonnullParameterIndex].getPosition();
          }
        };
      } else {
        positionHandler = new PositionHandler() {
          @Override
          SyntaxTree.NodePosition handle(final SyntaxTree.Node[] nodeParameters) {
            return SyntaxTree.NodePosition.of(
                nodeParameters[theFirstNonnullParameterIndex].getPosition().getStart(),
                nodeParameters[theLastNonnullParameterIndex].getPosition().getEnd()
            );
          }
        };
      }
      this.clazz = clazz;
      this.nonnullParameters = nonnullParameters.toArray(new NodeConstructorParameter[0]);
      this.positionHandler = positionHandler;
    }

    private final Class<TNode> clazz;

    private final NodeConstructorParameter[] nonnullParameters;

    final PositionHandler positionHandler;

    int getNonnullParameterCount() {
      return this.nonnullParameters.length;
    }

    NodeConstructorParameter getNonnullParameter(final int parameterIndex) {
      return this.nonnullParameters[parameterIndex];
    }

    abstract void construct(NodeAnalyseStack nodeAnalyseStack, LexicalIterator lexicalIterator, NodeConstructStack nodeConstructStack);

    @Override
    public String toString() {
      return this.clazz.getSimpleName() + "\t" + Arrays.toString(this.nonnullParameters);
    }

    private static abstract class PositionHandler {

      abstract SyntaxTree.NodePosition handle(SyntaxTree.Node[] nodeParameters);

    }

  }

  private static abstract class NodeConstructorParameter {

    private static final NodeConstructorParameter NULL = new NodeConstructorParameter(null, null, null) {
      @Override
      void construct(
          final NodeAnalyseStack nodeAnalyseStack,
          final LexicalIterator lexicalIterator,
          final NodeConstructStack nodeConstructStack
      ) {
        nodeConstructStack.push(null);
      }

      @Override
      public String toString() {
        return "null";
      }
    };

    static NodeConstructorParameter forNull() {
      return NULL;
    }

    static NodeConstructorParameter forNode(final Class<? extends SyntaxTree.Node> clazz) {
      return new NodeConstructorParameter(clazz, null, null) {
        @Override
        void construct(
            final NodeAnalyseStack nodeAnalyseStack,
            final LexicalIterator lexicalIterator,
            final NodeConstructStack nodeConstructStack
        ) {
          nodeAnalyseStack.pop().construct(nodeAnalyseStack, lexicalIterator, nodeConstructStack);
        }

        @Override
        public String toString() {
          return this.clazz.getSimpleName();
        }
      };
    }

    static NodeConstructorParameter forNodeList(
        final Class<? extends SyntaxTree.Node> clazz,
        final Class<? extends SyntaxTree.Node> elementClass,
        final Class<? extends Lexical> delimiterClass
    ) {
      return new NodeConstructorParameter(clazz, elementClass, delimiterClass) {
        @SuppressWarnings("unchecked")
        @Override
        void construct(
            final NodeAnalyseStack nodeAnalyseStack,
            final LexicalIterator lexicalIterator,
            final NodeConstructStack nodeConstructStack
        ) {
          int elementCount = 0;
          Lexical delimiter = null;
          if (this.delimiterClass == Lexical.Whitespace.class) {
            while (true) {
              // 分析一个列表元素
              NodeConstructor<?> topConstructor = nodeAnalyseStack.top();
              if (!this.elementClass.isAssignableFrom(topConstructor.clazz)) {
                break;
              }
              nodeAnalyseStack.pop();
              topConstructor.construct(nodeAnalyseStack, lexicalIterator, nodeConstructStack);
              elementCount++;
            }
            if (elementCount > 1) {
              // 需要分隔符
              delimiter = Lexical.Whitespace.INSTANCE;
            }
          } else {
            while (true) {
              NodeConstructor<?> topConstructor = nodeAnalyseStack.top();
              if (!this.elementClass.isAssignableFrom(topConstructor.clazz)) {
                throw new RuntimeException();
              }
              nodeAnalyseStack.pop();
              topConstructor.construct(nodeAnalyseStack, lexicalIterator, nodeConstructStack);
              elementCount++;
              try {
                topConstructor = nodeAnalyseStack.top();
              } catch (EmptyStackException ignore) {
                break;
              }
              if (!this.delimiterClass.isAssignableFrom(topConstructor.clazz)) {
                break;
              }
              nodeAnalyseStack.pop();
              topConstructor.construct(nodeAnalyseStack, lexicalIterator, nodeConstructStack);
              delimiter = (Lexical<?>) nodeConstructStack.pop();// 将delimiter从构造栈中去除
            }
          }
          if (elementCount == 0) {
            nodeConstructStack.push(null);
          } else {
            final SyntaxTree.NodeList nodeList;
            final SyntaxTree.NodePosition nodeListPosition;
            if (delimiter == null) {
              final SyntaxTree.Node element = nodeConstructStack.pop();
              nodeList = new SyntaxTree.NodeList<>(element);
              nodeListPosition = element.getPosition();
            } else {
              final SyntaxTree.Node[] elements = new SyntaxTree.Node[elementCount];
              nodeConstructStack.pop(elements);
              nodeList = new SyntaxTree.NodeList(Arrays.asList(elements), delimiter);
              nodeListPosition = SyntaxTree.NodePosition.of(
                  elements[0].getPosition().getStart(),
                  elements[elements.length - 1].getPosition().getEnd()
              );
            }
            nodeList.setPosition(nodeListPosition);
            nodeConstructStack.push(nodeList);
          }
        }

        @Override
        public String toString() {
          return this.clazz.getSimpleName() + "<" + this.elementClass.getSimpleName() + ", " + this.delimiterClass.getSimpleName() + ">";
        }
      };
    }

    private NodeConstructorParameter(
        final Class<? extends SyntaxTree.Node> clazz,
        final Class<? extends SyntaxTree.Node> elementClass,
        final Class<? extends Lexical> delimiterClass
    ) {
      this.clazz = clazz;
      this.elementClass = elementClass;
      this.delimiterClass = delimiterClass;
    }

    final Class<? extends SyntaxTree.Node> clazz;

    final Class<? extends SyntaxTree.Node> elementClass;

    final Class<? extends Lexical> delimiterClass;

    boolean isNull() {
      return this.clazz == null;
    }

    abstract void construct(NodeAnalyseStack nodeAnalyseStack, LexicalIterator lexicalIterator, NodeConstructStack nodeConstructStack);

    @Override
    public abstract String toString();

  }

  private static final class NodeConstructStack {

    private static final int INITIALIZE_CAPACITY = 10;

    private SyntaxTree.Node[] list = new SyntaxTree.Node[INITIALIZE_CAPACITY];

    private int size = 0;

    void push(final SyntaxTree.Node node) {
      if (this.size == this.list.length) {
        this.growCapacity();
      }
      this.list[this.size] = node;
      this.size++;
    }

    SyntaxTree.Node pop() {
      if (this.size == 0) {
        throw new EmptyStackException();
      }
      final int topIndex = this.size - 1;
      final SyntaxTree.Node node = this.list[topIndex];
      this.list[topIndex] = null;
      this.size--;
      return node;
    }

    void pop(final SyntaxTree.Node[] nodeArray) {
      if (this.size == 0) {
        throw new EmptyStackException();
      }
      final int newSize = this.size - nodeArray.length;
      final int theLastIndex = this.size - 1;
      for (int index = 0; index < nodeArray.length; index++) {
        nodeArray[index] = this.list[theLastIndex - index];
      }
      Arrays.fill(this.list, newSize, this.size, null);
      this.size = newSize;
    }

    boolean isEmpty() {
      return this.size == 0;
    }

    void clear() {
      Arrays.fill(this.list, 0, this.size, null);
      this.size = 0;
    }

    private void growCapacity() {
      final SyntaxTree.Node[] list = new SyntaxTree.Node[this.list.length + INITIALIZE_CAPACITY];
      System.arraycopy(this.list, 0, list, 0, this.list.length);
      this.list = list;
    }

  }

}
