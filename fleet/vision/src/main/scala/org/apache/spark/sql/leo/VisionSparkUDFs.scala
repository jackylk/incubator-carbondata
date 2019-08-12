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

package org.apache.spark.sql.leo

import org.apache.spark.sql.SQLContext
import Utils.{fromBytes, toBytes}


object VisionSparkUDFs {

  /**
   * Utility method to register all Vision-related UDFs at once
   *
   * For more fine grained control over the parameters, consider registering UDFs manually
   * instead of calling this method
   *
   * @param sqlContext
   */
  def registerAll(sqlContext: SQLContext): Unit = {
    registerCrop(sqlContext)
    registerResize(sqlContext)
    registerRotate(sqlContext)
    registerRotateWithBgColor(sqlContext)
    registerAddNoise(sqlContext)
  }


  /**
   * Image crop
   *
   * @param sqlContext
   */
  def registerCrop(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("crop",
      (bytes: Array[Byte], x: Int, y: Int, w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val croppedImg = BasicTransformations.crop(img, x, y, w, h)
        toBytes(croppedImg)
      })
  }


  /**
   * Image resize
   *
   * @param sqlContext
   */
  def registerResize(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("resize",
      (bytes: Array[Byte], w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val resizedImg = BasicTransformations.resize(img, w, h)
        toBytes(resizedImg)
      })
  }


  /**
   * Image rotate (with white background color)
   *
   * @param sqlContext
   */
  def registerRotate(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rotate",
      (bytes: Array[Byte], angle: Int) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle)
        toBytes(rotatedImg)
      })
  }


  /**
   * Image rotate with custom background color
   *
   * @param sqlContext
   */
  def registerRotateWithBgColor(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("rotateWithBgColor",
      (bytes: Array[Byte], angle: Int, bgColor: String) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle, bgColor)
        toBytes(rotatedImg)
      })
  }


  /**
   * Add random noise to image
   *
   * @param sqlContext
   */
  def registerAddNoise(sqlContext: SQLContext): Unit = {
    sqlContext.udf.register("addRandomNoise",
      (bytes: Array[Byte]) => {
        val img = fromBytes(bytes)
        val noisyImg = DataAugmentor.addRandomNoise(img, 1, 0.5)
        toBytes(noisyImg)
      })
  }

}
