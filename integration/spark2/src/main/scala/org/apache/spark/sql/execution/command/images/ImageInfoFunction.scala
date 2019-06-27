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
package org.apache.spark.sql.execution.command.images

import java.io.ByteArrayInputStream
import javax.imageio.ImageIO
import javax.imageio.stream.MemoryCacheImageInputStream

case class ImageInfo(
  format: String,
  width: Integer,
  height: Integer
)

/**
 * Image handing udf class
 */

class ImageInfoFunction extends ((Array[Byte]) => ImageInfo) with Serializable {

  override def apply(content: Array[Byte]): ImageInfo = {
    val (width, height, formatName) = getImageInfo(content)
    ImageInfo(formatName, width, height)
  }

  def getImageInfo(content: Array[Byte]): (Integer, Integer, String) = {
    val stream = new MemoryCacheImageInputStream(new ByteArrayInputStream(content))
    val iter = ImageIO.getImageReaders(stream)
    while ( {iter.hasNext}) {
      val reader = iter.next
      try {
        stream.seek(0)
        reader.setInput(stream)
        val width = reader.getWidth(reader.getMinIndex)
        val height = reader.getHeight(reader.getMinIndex)
        val formatName = reader.getFormatName()
        return (width, height, formatName)
      } catch {
        // ignore the execption and continue for next reader
        case _ =>
      } finally reader.dispose()
    }
    (null, null, null)
  }
}
