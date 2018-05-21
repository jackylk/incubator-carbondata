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

package org.apache.carbondata.stream

import java.util.concurrent.{CountDownLatch, TimeUnit}

import scala.collection.mutable

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.StreamingQuery

import org.apache.carbondata.common.logging.LogServiceFactory
import org.apache.carbondata.core.metadata.schema.table.CarbonTable
import org.apache.carbondata.spark.StreamingOption
import org.apache.carbondata.streaming.CarbonStreamException

object StreamJobManager {
  private val LOGGER = LogServiceFactory.getLogService(this.getClass.getCanonicalName)
  private val jobs = mutable.Map[String, StreamJobDesc]()

  /**
   * Start a spark streaming query
   * @param sparkSession session instance
   * @param sourceTable stream source table
   * @param sinkTable sink table to insert to
   * @param query query string
   * @param streamDf dataframe that containing the query from stream source table
   * @param options options provided by user
   * @return Job ID
   */
  def startJob(
      sparkSession: SparkSession,
      sourceTable: CarbonTable,
      sinkTable: CarbonTable,
      query: String,
      streamDf: DataFrame,
      options: StreamingOption): String = {
    val latch = new CountDownLatch(1)
    var exception: Throwable = null
    var job: StreamingQuery = null

    // start a new thread to run the streaming ingest job, the job will be running
    // until user stops it by STOP STREAM JOB
    val thread = new Thread(new Runnable {
      override def run(): Unit = {
        try {
          job = streamDf.writeStream
            .format("carbondata")
            .trigger(options.trigger)
            .options(options.userInputMap)
            .option("checkpointLocation", options.checkpointLocation(sinkTable.getTablePath))
            .option("dateformat", options.dateFormat)
            .option("timestampformat", options.timeStampFormat)
            .option("carbon.stream.parser", options.rowParser)
            .option("dbName", sinkTable.getDatabaseName)
            .option("tableName", sinkTable.getTableName)
            .start()
          latch.countDown()
          job.awaitTermination()
        } catch {
          case e: Throwable =>
            LOGGER.error(e)
            exception = e
            latch.countDown()
        }
      }
    })
    thread.start()

    // wait for max 10 seconds for the streaming job to start
    if (latch.await(10, TimeUnit.SECONDS)) {
      if (exception != null) {
        throw exception
      }

      jobs(job.id.toString) =
        StreamJobDesc(job, sourceTable.getDatabaseName, sourceTable.getTableName,
          sinkTable.getDatabaseName, sinkTable.getTableName, query, thread)
      job.id.toString
    } else {
      thread.interrupt()
      throw new CarbonStreamException("Streaming job takes too long to start")
    }
  }

  def killJob(jobId: String): Unit = {
    if (jobs.contains(jobId)) {
      val jobDesc = jobs(jobId)
      jobDesc.streamingQuery.stop()
      jobDesc.thread.interrupt()
      jobs.remove(jobId)
    }
  }

  def getAllJobs: Set[StreamJobDesc] = jobs.values.toSet

  def getJobIdOnTable(carbonTable: CarbonTable): Option[String] = {
    val jobs = StreamJobManager.getAllJobs.filter { job =>
      job.sinkTable.equalsIgnoreCase(carbonTable.getTableName) &&
      job.sinkDb.equalsIgnoreCase(carbonTable.getDatabaseName)
    }.toSeq
    if (jobs.isEmpty) None else Some(jobs.head.streamingQuery.id.toString)
  }

}

private[stream] case class StreamJobDesc(
    streamingQuery: StreamingQuery,
    sourceDb: String,
    sourceTable: String,
    sinkDb: String,
    sinkTable: String,
    query: String,
    thread: Thread,
    startTime: Long = System.currentTimeMillis()
)
