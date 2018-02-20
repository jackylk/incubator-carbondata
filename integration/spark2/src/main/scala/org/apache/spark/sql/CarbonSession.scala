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
package org.apache.spark.sql

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable

import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.scheduler.{SparkListener, SparkListenerApplicationEnd}
import org.apache.spark.sql.SparkSession.Builder
import org.apache.spark.sql.execution.streaming.CarbonStreamingQueryListener
import org.apache.spark.sql.hive.execution.command.CarbonSetCommand
import org.apache.spark.sql.internal.{SessionState, SharedState}
import org.apache.spark.util.{CarbonReflectionUtils, Utils}

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.carbondata.core.util.{CarbonProperties, CarbonSessionInfo, ThreadLocalSessionInfo}
import org.apache.carbondata.datamap.MVDataMapRules
import org.apache.carbondata.datamap.preaggregate.PreaggregateMVDataMapRules
import org.apache.carbondata.hadoop.util.CarbonInputFormatUtil

/**
 * Session implementation for {org.apache.spark.sql.SparkSession}
 * Implemented this class only to use our own SQL DDL commands.
 * User needs to use {CarbonSession.getOrCreateCarbon} to create Carbon session.
 */
class CarbonSession(@transient val sc: SparkContext,
    @transient private val existingSharedState: Option[SharedState]) extends SparkSession(sc) {

  def this(sc: SparkContext) {
    this(sc, None)
  }

  @transient
  override lazy val sessionState: SessionState =
    CarbonReflectionUtils.getSessionState(sparkContext, this).asInstanceOf[SessionState]

  /**
   * State shared across sessions, including the `SparkContext`, cached data, listener,
   * and a catalog that interacts with external systems.
   */
  @transient
  override lazy val sharedState: SharedState = {
    existingSharedState match {
      case Some(_) =>
        val ss = existingSharedState.get
        if (ss == null) {
          new SharedState(sparkContext)
        } else {
          ss
        }
      case None =>
        new SharedState(sparkContext)
    }
  }

  override def newSession(): SparkSession = {
    new CarbonSession(sparkContext, Some(sharedState))
  }

  // materialized view rules, currently one datamap is supported: PreaggregateDataMap
  private var mvDataMapRules: mutable.Seq[MVDataMapRules] = mutable.Seq()

  def addMVDataMapRules(rules: Seq[MVDataMapRules]): Unit =
    mvDataMapRules = mvDataMapRules ++ rules

  def getMVDataMapRules: Seq[MVDataMapRules] = mvDataMapRules
}

object CarbonSession {

  implicit class CarbonBuilder(builder: Builder) {

    private var mvDataMap: mutable.Seq[MVDataMapRules] = mutable.Seq()

    def getOrCreateCarbonSession(): SparkSession = {
      getOrCreateCarbonSession(null, null)
    }

    def getOrCreateCarbonSession(storePath: String): SparkSession = {
      getOrCreateCarbonSession(
        storePath,
        new File(CarbonCommonConstants.METASTORE_LOCATION_DEFAULT_VAL).getCanonicalPath)
    }

    def getOrCreateCarbonSession(storePath: String,
        metaStorePath: String): SparkSession = synchronized {
      builder.enableHiveSupport()
      val options =
        getValue("options", builder).asInstanceOf[scala.collection.mutable.HashMap[String, String]]
      val userSuppliedContext: Option[SparkContext] =
        getValue("userSuppliedContext", builder).asInstanceOf[Option[SparkContext]]

      if (metaStorePath != null) {
        val hadoopConf = new Configuration()
        val configFile = Utils.getContextOrSparkClassLoader.getResource("hive-site.xml")
        if (configFile != null) {
          hadoopConf.addResource(configFile)
        }
        if (options.get(CarbonCommonConstants.HIVE_CONNECTION_URL).isEmpty &&
            hadoopConf.get(CarbonCommonConstants.HIVE_CONNECTION_URL) == null) {
          val metaStorePathAbsolute = new File(metaStorePath).getCanonicalPath
          val hiveMetaStoreDB = metaStorePathAbsolute + "/metastore_db"
          options ++= Map[String, String]((CarbonCommonConstants.HIVE_CONNECTION_URL,
            s"jdbc:derby:;databaseName=$hiveMetaStoreDB;create=true"))
        }
      }

      // Get the session from current thread's active session.
      var session: CarbonSession = SparkSession.getActiveSession match {
        case Some(sparkSession: CarbonSession) =>
          if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
            options.foreach { case (k, v) => sparkSession.sessionState.conf.setConfString(k, v) }
            sparkSession
          } else {
            null
          }
        case _ => null
      }
      if (session ne null) {
        return session
      }

      // Global synchronization so we will only set the default session once.
      SparkSession.synchronized {
        // If the current thread does not have an active session, get it from the global session.
        session = SparkSession.getDefaultSession match {
          case Some(sparkSession: CarbonSession) =>
            if ((sparkSession ne null) && !sparkSession.sparkContext.isStopped) {
              options.foreach { case (k, v) => sparkSession.sessionState.conf.setConfString(k, v) }
              sparkSession
            } else {
              null
            }
          case _ => null
        }
        if (session ne null) {
          return session
        }

        // No active nor global default session. Create a new one.
        val sparkContext = userSuppliedContext.getOrElse {
          // set app name if not given
          val randomAppName = java.util.UUID.randomUUID().toString
          val sparkConf = new SparkConf()
          options.foreach { case (k, v) => sparkConf.set(k, v) }
          if (!sparkConf.contains("spark.app.name")) {
            sparkConf.setAppName(randomAppName)
          }
          val sc = SparkContext.getOrCreate(sparkConf)
          CarbonInputFormatUtil.setS3Configurations(sc.hadoopConfiguration)
          // maybe this is an existing SparkContext, update its SparkConf which maybe used
          // by SparkSession
          options.foreach { case (k, v) => sc.conf.set(k, v) }
          if (!sc.conf.contains("spark.app.name")) {
            sc.conf.setAppName(randomAppName)
          }
          sc
        }

        session = new CarbonSession(sparkContext)
        val carbonProperties = CarbonProperties.getInstance()
        if (storePath != null) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION, storePath)
          // In case if it is in carbon.properties for backward compatible
        } else if (carbonProperties.getProperty(CarbonCommonConstants.STORE_LOCATION) == null) {
          carbonProperties.addProperty(CarbonCommonConstants.STORE_LOCATION,
            session.sessionState.conf.warehousePath)
        }
        options.foreach { case (k, v) => session.sessionState.conf.setConfString(k, v) }
        SparkSession.setDefaultSession(session)
        // Register a successfully instantiated context to the singleton. This should be at the
        // end of the class definition so that the singleton is updated only if there is no
        // exception in the construction of the instance.
        sparkContext.addSparkListener(new SparkListener {
          override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
            SparkSession.setDefaultSession(null)
            SparkSession.sqlListener.set(null)
          }
        })
        session.streams.addListener(new CarbonStreamingQueryListener(session))
        addDataMapRules(session)
      }

      session
    }

    private def addDataMapRules(session: CarbonSession) = {
      // by default, enable PreaggregateDataMap
      if (mvDataMap.isEmpty) {
        mvDataMap = mvDataMap :+ new PreaggregateMVDataMapRules
      }
      session.addMVDataMapRules(mvDataMap)
    }

    def enableMVDataMap(mvDataMapRules: MVDataMapRules): CarbonBuilder = {
      mvDataMap = mvDataMap :+ mvDataMapRules
      this
    }

    /**
     * It is a hack to get the private field from class.
     */
    private def getValue(name: String, builder: Builder): Any = {
      val currentMirror = scala.reflect.runtime.currentMirror
      val instanceMirror = currentMirror.reflect(builder)
      val m = currentMirror.classSymbol(builder.getClass).
        toType.members.find { p =>
        p.name.toString.equals(name)
      }.get.asTerm
      instanceMirror.reflectField(m).get
    }
  }

  def threadSet(key: String, value: String): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    val threadParams = currentThreadSessionInfo.getThreadParams
    CarbonSetCommand.validateAndSetValue(threadParams, key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }


  def threadSet(key: String, value: Object): Unit = {
    var currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo == null) {
      currentThreadSessionInfo = new CarbonSessionInfo()
    }
    else {
      currentThreadSessionInfo = currentThreadSessionInfo.clone()
    }
    currentThreadSessionInfo.getThreadParams.setExtraInfo(key, value)
    ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfo)
  }

  def threadUnset(key: String): Unit = {
    val currentThreadSessionInfo = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfo != null) {
      val currentThreadSessionInfoClone = currentThreadSessionInfo.clone()
      val threadParams = currentThreadSessionInfoClone.getThreadParams
      CarbonSetCommand.unsetValue(threadParams, key)
      threadParams.removeExtraInfo(key)
      ThreadLocalSessionInfo.setCarbonSessionInfo(currentThreadSessionInfoClone)
    }
  }

  def updateSessionInfoToCurrentThread(sparkSession: SparkSession): Unit = {
    val carbonSessionInfo = CarbonEnv.getInstance(sparkSession).carbonSessionInfo.clone()
    val currentThreadSessionInfoOrig = ThreadLocalSessionInfo.getCarbonSessionInfo
    if (currentThreadSessionInfoOrig != null) {
      val currentThreadSessionInfo = currentThreadSessionInfoOrig.clone()
      // copy all the thread parameters to apply to session parameters
      currentThreadSessionInfo.getThreadParams.getAll.asScala
        .foreach(entry => carbonSessionInfo.getSessionParams.addProperty(entry._1, entry._2))
      carbonSessionInfo.setThreadParams(currentThreadSessionInfo.getThreadParams)
    }
    // preserve thread parameters across call
    ThreadLocalSessionInfo.setCarbonSessionInfo(carbonSessionInfo)
  }

}
