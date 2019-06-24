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

package org.apache.carbondata.cloud

import scala.collection.mutable.ArrayBuffer

import com.google.gson.{JsonObject, JsonParser}
import com.huawei.ais.common.AuthInfo
import com.huawei.ais.sdk.AisAccess
import org.apache.commons.codec.binary.Base64
import org.apache.commons.io.IOUtils
import org.apache.http.entity.StringEntity
import org.apache.spark.sql.SparkSession

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.util.CarbonProperties

/**
 * util of OCR Udf
 */
object OCRUdf {

  val LOGGER = LogServiceFactory.getLogService(OCRUdf.getClass.getCanonicalName)

  /**
   * register all OCR UDF which are configured in carbon.properties
   */
  def register(sparkSession: SparkSession): Unit = {
    val (endPoint, region, ak, sk) = CloudUdfRegister.getCloudConf(sparkSession)
    udfs.map { udf =>
      sparkSession.udf.register(udf._1, (image: Array[Byte]) => {
        initService(endPoint, region, ak, sk)
        postBytes(udf._2, image)
      })
      LOGGER.info(s"register OCR UDF: ${udf._1} with uri: ${udf._2} into spark")
    }
  }

  /**
   * each thread will reuse AuthInfo and AisAccess object
   */
  private val service = new ThreadLocal[AisAccess]()
  private val lock = new Object()

  // TODO need to optimize the lock mechanism
  private def initService(endpoint: String, region: String, ak: String, sk: String): Unit = {
    lock.synchronized {
      if (service.get() == null) {
        service.set(new AisAccess(new AuthInfo(endpoint, region, ak, sk)))
      }
    }
  }

  def releaseService(): Unit = {
    lock.synchronized {
      if (service.get() != null) {
        service.get().close()
      }
    }
  }

  private def postBytes(uri: String, image: Array[Byte]): String = {
    val fileBase64Str = Base64.encodeBase64String(image)
    val jsonObject = new JsonObject()
    jsonObject.addProperty("image", fileBase64Str)
    // service should be initialized
    val response = service.get().post(
      uri,
      new StringEntity(jsonObject.toString(), "utf-8"))
    if (response == null) {
      null
    } else {
      val content = IOUtils.toString(response.getEntity.getContent)
      val jo = new JsonParser().parse(content).getAsJsonObject
      if (jo.has("result")) {
        jo.get("result").toString
      } else {
        null
      }
    }
  }

  /**
   * list all udf information: (udf name, ocr uri)
   */
  private def udfs(): Array[(String, String)] = {
    val iterator = CarbonProperties.getInstance().iterator()
    val udfArray = new ArrayBuffer[(String, String)]()
    val prefixLength = CloudConstants.CLOUD_UDF_OCR_PREFIX.size
    while (iterator.hasNext) {
      val entry = iterator.next()
      val key = entry.getKey().asInstanceOf[String]
      if (key.startsWith(CloudConstants.CLOUD_UDF_OCR_PREFIX)) {
        udfArray += ((key.substring(prefixLength), entry.getValue.asInstanceOf[String]))
      }
    }
    udfArray.toArray
  }
}
