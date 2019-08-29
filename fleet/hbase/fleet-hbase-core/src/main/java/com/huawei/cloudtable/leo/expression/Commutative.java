package com.huawei.cloudtable.leo.expression;

/**
 * 标识一个表达是否满足交换律。
 * 继承了该接口的表达式，有且只有两个入参。
 * 一个满足交换律的表达式，将会生成两个表达式定义。
 */
public interface Commutative<TParameter1, TParameter2> {

  Class<TParameter1> getParameter1Class();

  Class<TParameter2> getParameter2Class();

  Evaluation<TParameter1> getParameter1();

  Evaluation<TParameter2> getParameter2();

  default int getHashCode() {
    return this.getParameter1().hashCode() + this.getParameter2().hashCode();
  }

  default boolean equals(final Commutative that) {
    if (this.getParameter1().equals(that.getParameter1()) && this.getParameter2().equals(that.getParameter2())) {
      return true;
    }
    return (this.getParameter1().equals(that.getParameter2()) && this.getParameter2().equals(that.getParameter1()));
  }

}
