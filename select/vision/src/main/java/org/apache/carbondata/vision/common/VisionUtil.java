/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.carbondata.vision.common;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Properties;

import org.apache.carbondata.common.logging.LogService;
import org.apache.carbondata.common.logging.LogServiceFactory;

public class VisionUtil {

  private static LogService LOGGER = LogServiceFactory.getLogService(VisionUtil.class.getName());

  public static final Charset DEFAULT_CHARSET = Charset.forName("UTF-8");

  public static float[] getTopN(float[] result, int topN) {
    if (result.length > topN) {
      float greatestFloat = findNthGreatestFloat(result, result.length, topN);
      float[] topResult = new float[topN];
      int count = 0;
      for (int i = 0; i < result.length; i++) {
        if (result[i] >= greatestFloat) {
          topResult[count++] = result[i];
        }
        if (count >= topN) {
          break;
        }
      }
      return topResult;
    }
    return result;
  }

  public static float findNthGreatestFloat(float[] values, int length, int topN) {
    float tmp = values[length / 2];
    int equalValuesCount = 0;
    float[] lessValues = new float[length];
    int lessValuesPoint = 0;
    float[] greaterValues = new float[length];
    int greaterValuesPoint = 0;
    for (int i = 0; i < length; i++) {
      if (values[i] < tmp) {
        lessValues[lessValuesPoint++] = values[i];
      } else if (values[i] > tmp) {
        greaterValues[greaterValuesPoint++] = values[i];
      } else {
        equalValuesCount++;
      }
    }
    if (greaterValuesPoint > topN) {
      return findNthGreatestFloat(greaterValues, greaterValuesPoint, topN);
    } else if (greaterValuesPoint + equalValuesCount >= topN) {
      return tmp;
    } else {
      return findNthGreatestFloat(lessValues, lessValuesPoint,
          topN - greaterValuesPoint - equalValuesCount);
    }
  }

  public static Object newInstanceByName(String className, Class[] paramTypes, Object[] paramObjs) {
    try {
      Class cls = Class.forName(className);
      Constructor constructor = cls.getConstructor(paramTypes);
      return constructor.newInstance(paramObjs);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    } catch (NoSuchMethodException e) {
      LOGGER.error(e);
    } catch (IllegalAccessException e) {
      LOGGER.error(e);
    } catch (InstantiationException e) {
      LOGGER.error(e);
    } catch (InvocationTargetException e) {
      LOGGER.error(e);
    }
    return null;
  }

  public static byte[] serialize(Object object) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream(1024);
    try {
      ObjectOutputStream oos = new ObjectOutputStream(baos);
      oos.writeObject(object);
    } catch (IOException e) {
      LOGGER.error(e);
    }
    return baos.toByteArray();
  }

  public static Object deserialize(byte[] bytes) {
    if (bytes == null) {
      return null;
    }
    try {
      ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(bytes));
      return ois.readObject();
    } catch (IOException e) {
      LOGGER.error(e);
    } catch (ClassNotFoundException e) {
      LOGGER.error(e);
    }
    return null;
  }

  public static void loadProperties(String filePath, VisionConfiguration conf) {
    InputStream input = null;
    try {
      input = new FileInputStream(filePath);
      Properties prop = new Properties();
      prop.load(input);
      for (Map.Entry<Object, Object> entry : prop.entrySet()) {
        conf.conf(entry.getKey().toString(), entry.getValue().toString());
      }
      LOGGER.audit("loaded properties: " + filePath);
    } catch (IOException ex) {
      LOGGER.error(ex, "Failed to load properties file " + filePath);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException e) {
          LOGGER.error(e);
        }
      }
    }
  }

  public static String printlnTime(long... times) {
    if (times.length >= 2) {
      StringBuilder builder = new StringBuilder();
      int i = 1;
      for (; i < times.length; i++) {
        builder.append("t" + i + "~(").append(times[i] - times[i - 1]).append("ms)~");
      }
      builder.append("t" + i);
      return builder.toString();
    }
    return "need more than a time";
  }
}
