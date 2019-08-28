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

package org.apache.spark.sql.leo.command

import com.huawei.cloud.obs.OBSUtil
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.execution.command.{CreateDatabaseCommand, DropDatabaseCommand, RunnableCommand}
import org.apache.spark.sql.leo.LeoEnv
import org.apache.spark.sql.leo.hbase.HBaseUtil
import org.apache.spark.util.FileUtils

import org.apache.carbondata.core.datastore.impl.FileFactory

// create database should create OBS bucket
case class LeoCreateDatabaseCommand(sparkCommand: CreateDatabaseCommand)
  extends RunnableCommand {
  override val output: Seq[Attribute] = sparkCommand.output
  val dbName: String = sparkCommand.databaseName
  val bucket: String = LeoEnv.bucketName(dbName)
  val ifNotExists: Boolean = sparkCommand.ifNotExists

  override def run(sparkSession: SparkSession): Seq[Row] = {
    // step 1. create data folder for database, it can be bucket or FS folder
    val path = sparkCommand.path.getOrElse(LeoEnv.getDefaultDBPath(dbName, sparkSession))
    val regionName = sparkSession.conf.get("carbon.source.region")
    FileFactory.getFileType(path) match {

      case FileFactory.FileType.OBS => OBSUtil.createBucket(bucket, sparkSession, regionName)
      case _ => FileUtils.createDatabaseDirectory(dbName, path, sparkSession.sparkContext)
    }

    // step 2. create db meta in hive metastore
    try {
      CreateDatabaseCommand(
        sparkCommand.databaseName,
        sparkCommand.ifNotExists,
        Some(path),
        sparkCommand.comment,
        sparkCommand.props
      ).run(sparkSession)
    } catch {
        // if db exists, no need to clean up bucket
      case ex: AnalysisException
        if ("org.apache.hadoop.hive.metastore.api.AlreadyExistsException: Database " +
          sparkCommand.databaseName + " already exists;").equals(ex.getMessage()) =>
          throw ex
      case e: Throwable =>
        //  clean up bucket
        OBSUtil.deleteBucket(bucket, true, sparkSession)
        throw e
    }

    //TODO make hbase configurable in leo, if hbase enable, them create ns.
    // step 3. create namespace in hbase
//    try {
//      HBaseUtil.createNameSpace(dbName, ifNotExists, sparkSession)
//    } catch {
//      case e: Throwable =>
//        // drop in hive metastore
//        DropDatabaseCommand(
//          sparkCommand.databaseName,
//          sparkCommand.ifNotExists,
//          cascade = true
//        ).run(sparkSession)
//
//        //  clean up bucket
//        OBSUtil.deleteBucket(bucket, true, sparkSession)
//        throw e
//    }

    Seq.empty
  }


}
