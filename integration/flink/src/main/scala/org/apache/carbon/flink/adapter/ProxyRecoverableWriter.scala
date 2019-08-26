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

import org.apache.carbon.flink.adapter
import org.apache.flink.core.fs.{Path, RecoverableFsDataOutputStream, RecoverableWriter}
import org.apache.flink.core.io.SimpleVersionedSerializer

class ProxyRecoverableWriter extends RecoverableWriter {

  override def open(path: Path): RecoverableFsDataOutputStream = {
    new ProxyRecoverableOutputStream
  }

  override def recover(resumeRecoverable: RecoverableWriter.ResumeRecoverable): RecoverableFsDataOutputStream = {
    throw new UnsupportedOperationException
  }

  override def requiresCleanupOfRecoverableState(): Boolean = {
    false
  }

  override def cleanupRecoverableState(resumeRecoverable: RecoverableWriter.ResumeRecoverable): Boolean = {
    throw new UnsupportedOperationException
  }

  override def recoverForCommit(recoverable: RecoverableWriter.CommitRecoverable): RecoverableFsDataOutputStream.Committer = {
    recoverable match {
      case proxyRecoverable: ProxyRecoverable =>
        new adapter.ProxyRecoverableOutputStream.Committer(proxyRecoverable, null)
      case _ =>
        throw new IllegalArgumentException("ProxyFileSystem cannot recover recoverable for other file system: " + recoverable)
    }
  }

  override def getCommitRecoverableSerializer: SimpleVersionedSerializer[RecoverableWriter.CommitRecoverable] = {
    ProxyRecoverableSerializer.INSTANCE.asInstanceOf[SimpleVersionedSerializer[RecoverableWriter.CommitRecoverable]]
  }

  override def getResumeRecoverableSerializer: SimpleVersionedSerializer[RecoverableWriter.ResumeRecoverable] = {
    ProxyRecoverableSerializer.INSTANCE.asInstanceOf[SimpleVersionedSerializer[RecoverableWriter.ResumeRecoverable]]
  }

  override def supportsResume(): Boolean = {
    false
  }

}
