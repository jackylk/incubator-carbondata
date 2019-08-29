package com.huawei.cloudtable.leo.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public final class CollectionHelper {

  public static boolean isAllNull(final Object[] list) {
    for (int index = 0; index < list.length; index++) {
      if (list[index] != null) {
        return false;
      }
    }
    return true;
  }

  public static boolean isAllNull(final List list) {
    for (int index = 0; index < list.size(); index++) {
      if (list.get(index) != null) {
        return false;
      }
    }
    return true;
  }

  public static <TElement> TElement getLast(final TElement[] list) {
    return list[list.length - 1];
  }

  public static <TElement> TElement getLast(final List<TElement> list) {
    return list.get(list.size() - 1);
  }

  public static <TElement> void setLast(final TElement[] list, final TElement element) {
    list[list.length - 1] = element;
  }

  public static <TElement> List<TElement> invert(final List<TElement> list) {
    final List<TElement> invertedList = new ArrayList<>(list.size());
    for (int index = list.size() - 1; index >= 0; index--) {
      invertedList.add(list.get(index));
    }
    return invertedList;
  }

  public static Properties duplicate(final Properties source) {
    final Properties target = new Properties();
    for (final String propertyName : source.stringPropertyNames()) {
      target.setProperty(propertyName, source.getProperty(propertyName));
    }
    return target;
  }

  public static <TElement> void copy(
      final List<TElement> source,
      final int sourceOffset,
      final List<TElement> target,
      final int targetOffset,
      final int length
  ) {
    for (int index = 0; index < length; index++) {
      final int targetIndex = targetOffset + index;
      while (target.size() <= targetIndex) {
        target.add(null);// 先扩容
      }
      target.set(targetIndex, source.get(sourceOffset + index));
    }
  }

  private CollectionHelper() {
    // to do nothing.
  }

}
