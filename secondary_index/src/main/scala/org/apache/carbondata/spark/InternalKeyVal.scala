/*
 *
 * Copyright Notice
 * ===================================================================
 * This file contains proprietary information of
 * Huawei Technologies India Pvt Ltd.
 * Redistribution or use without prior written approval is prohibited.
 * Copyright (c) 2018
 * ===================================================================
 *
 */
package org.apache.carbondata.spark

/**
 *
 */
trait SecondaryIndexCreationResult[K, V] extends Serializable {
  def getKey(key: String, value: Boolean): (K, V)
}

class SecondaryIndexCreationResultImpl extends SecondaryIndexCreationResult[String, Boolean] {
  override def getKey(key: String, value: Boolean): (String, Boolean) = (key, value)
}
