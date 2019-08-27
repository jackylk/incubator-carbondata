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
package org.apache.carbon.flink.adapter

import org.apache.flink.api.common.serialization.BulkWriter
import org.apache.flink.core.fs.RecoverableWriter.CommitRecoverable
import org.apache.flink.core.fs.{RecoverableFsDataOutputStream, RecoverableWriter}

class ProxyRecoverableOutputStream extends RecoverableFsDataOutputStream {

  private var writer: BulkWriter[_] = _

  def setWriter(writer: BulkWriter[_]): Unit = {
    this.writer = writer
  }

  override def getPos: Long = {
    throw new UnsupportedOperationException
  }

  override def sync(): Unit = {
    throw new UnsupportedOperationException
  }

  override def write(byte: Int): Unit = {
    throw new UnsupportedOperationException
  }

  override def flush(): Unit = {
    throw new UnsupportedOperationException
  }

  override def close(): Unit = {
    this.writer.finish()
  }

  override def persist(): RecoverableWriter.ResumeRecoverable = {
    throw new UnsupportedOperationException
  }

  override def closeForCommit(): RecoverableFsDataOutputStream.Committer = {
    new ProxyRecoverableOutputStream.Committer(new ProxyRecoverable, this.writer)
  }

}

object ProxyRecoverableOutputStream {

  class Committer(recoverable: ProxyRecoverable, writer: BulkWriter[_]) extends RecoverableFsDataOutputStream.Committer {

    override def commit(): Unit = {
      if (writer != null) {
        writer.finish()
      }
    }

    override def commitAfterRecovery(): Unit = {
      throw new UnsupportedOperationException
    }

    override def getRecoverable: RecoverableWriter.CommitRecoverable = {
      recoverable
    }

  }

}
