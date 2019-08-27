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

import java.net.URI

import org.apache.flink.core.fs._

class ProxyFileSystem extends FileSystem {

  override def getWorkingDirectory: Path = {
    throw new UnsupportedOperationException
  }

  override def getHomeDirectory: Path = {
    throw new UnsupportedOperationException
  }

  override def getUri: URI = {
    ProxyFileSystem.DEFAULT_URI
  }

  override def getFileStatus(path: Path): FileStatus = {
    throw new UnsupportedOperationException
  }

  override def getFileBlockLocations(fileStatus: FileStatus, offset: Long, length: Long): Array[BlockLocation] = {
    throw new UnsupportedOperationException
  }

  override def open(path: Path, bufferSize: Int): FSDataInputStream = {
    throw new UnsupportedOperationException
  }

  override def open(path: Path): FSDataInputStream = {
    throw new UnsupportedOperationException
  }

  override def listStatus(path: Path): Array[FileStatus] = {
    throw new UnsupportedOperationException
  }

  override def delete(path: Path, b: Boolean): Boolean = {
    throw new UnsupportedOperationException
  }

  override def mkdirs(path: Path): Boolean = {
    throw new UnsupportedOperationException
  }

  override def create(path: Path, writeMode: FileSystem.WriteMode): FSDataOutputStream = {
    throw new UnsupportedOperationException
  }

  override def createRecoverableWriter(): RecoverableWriter = {
    new ProxyRecoverableWriter
  }

  override def rename(path: Path, path1: Path): Boolean = {
    throw new UnsupportedOperationException
  }

  override def isDistributedFS: Boolean = {
    false
  }

  override def getKind: FileSystemKind = {
    FileSystemKind.FILE_SYSTEM
  }

}

object ProxyFileSystem {

  val DEFAULT_URI = URI.create(ProxyFileSystemFactory.SCHEMA + ":/")

  val INSTANCE = new ProxyFileSystem

}
