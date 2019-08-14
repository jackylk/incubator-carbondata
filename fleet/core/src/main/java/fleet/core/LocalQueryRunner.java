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

package fleet.core;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import leo.job.QueryDef.QueryType;
import leo.model.view.SqlResult;
import leo.util.SqlResultUtil;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import leo.job.AsyncJob;
import leo.job.CarbonAsyncJob;
import leo.job.CarbonAsyncQueryInternal;
import leo.job.JobMeta;
import leo.job.JobMetaStoreClient;
import leo.job.Query;
import leo.qs.intf.QueryRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.command.RunnableCommand;
import org.apache.spark.sql.leo.util.OBSUtil;
import org.apache.spark.sql.util.SparkSQLUtil;

public class LocalQueryRunner implements QueryRunner {

  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LocalQueryRunner.class.getName());
  private static final String TIME_STAMP_FORMAT = "timestampformat";
  private static final String DATE_FORMAT = "dateformat";
  private static final int RESULT_PARTITION_NUM = 1;
  private static final int RESULT_LIMIT_NUM =
      CarbonProperties.getInstance().getAsyncQueryResLimit();

  private SparkSession sparkSession;
  private JobMetaStoreClient metaClient;

  private ArrayBlockingQueue<CarbonAsyncQueryInternal> carbonAsyncQueries =
      new ArrayBlockingQueue<CarbonAsyncQueryInternal>(4096);
  private Cache<String, JobMeta> runningJobsCache =
      CacheBuilder.newBuilder().maximumSize(2048).expireAfterWrite(2, TimeUnit.HOURS)
          .build();
  private Cache<String, JobMeta> finishedJobsCache =
      CacheBuilder.newBuilder().maximumSize(2048).expireAfterWrite(2, TimeUnit.HOURS)
          .build();
  private Cache<String, JobMeta> failedJobsCache =
      CacheBuilder.newBuilder().maximumSize(2048).expireAfterWrite(2, TimeUnit.HOURS)
          .build();
  private ThreadPoolExecutor carbonQueryPool;

  public LocalQueryRunner() {

  }

  public LocalQueryRunner(SparkSession sparkSession, JobMetaStoreClient metaStoreClient) {
    this.carbonQueryPool =
        new ThreadPoolExecutor(10, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(1024));
    carbonQueryPool.setThreadFactory(
        new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Carbon query thread #%d")
            .build());
    for (int i = 0; i < carbonQueryPool.getCorePoolSize(); i++) {
      carbonQueryPool.submit(new CarbonQueryTask());
    }
    this.metaClient = metaStoreClient;
    this.sparkSession = sparkSession;
    final Configuration configuration = new Configuration();
  }

  @Override
  public AsyncJob doAsyncJob(Query query, JobMeta jobMeta) {
    runningJobsCache.put(jobMeta.getJobId(), jobMeta);
    //handle async
    carbonAsyncQueries.offer(new CarbonAsyncQueryInternal(
        jobMeta.getJobId(), query.getResultPath(), query, jobMeta.getProjectId()));
    //return response asap.
    return new CarbonAsyncJob(jobMeta.getJobId());
  }

  @Override
  public JobMeta getJobMeta(String jobId, String projectId) {
    JobMeta jobMeta;
    if (runningJobsCache.getIfPresent(jobId) != null) {
      return runningJobsCache.getIfPresent(jobId);
    } else if (finishedJobsCache.getIfPresent(jobId) != null) {
      return finishedJobsCache.getIfPresent(jobId);
    } else if (failedJobsCache.getIfPresent(jobId) != null) {
      return failedJobsCache.getIfPresent(jobId);
    } else if ((jobMeta = getJobMetaFromMetaStore(jobId, projectId)) != null) {
      return jobMeta;
    }

    return null;
  }

  @Override
  public List<String[]> fetchResultPage(String path, int startLineNum, int limit)
      throws IOException {
    return OBSUtil.readObsFileByPagesCsvFormat(path, sparkSession, startLineNum, limit);
  }

  public JobMeta getJobMetaFromMetaStore(String jobId, String projectId) {
    JobMeta jobMeta = metaClient.getJobStatus(jobId, projectId);
    return jobMeta;
  }

  private class CarbonQueryTask implements Runnable {
    @Override
    public void run() {
      while (true) {
        if (sparkSession != null) {
          String jobID = null;
          String projectId = null;
          try {
            CarbonAsyncQueryInternal carbonQuery = carbonAsyncQueries.take();
            jobID = carbonQuery.getJobId();
            projectId = carbonQuery.getProjectId();
            LOGGER.info(Thread.currentThread().getName() + " job started: " + jobID);
            QueryType type = carbonQuery.getQuery().getTypeDef().getType();
            if (type.equals(QueryType.CARBON_SELECT) || type.equals(QueryType.RUN_SCRIPT)) {
              // if carbon select, we should execute it and save the query result.
              String timestampFormat = CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT;
              String dateFormat = CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT;
              if (carbonQuery.getQuery().getTblProperties() != null) {
                if (carbonQuery.getQuery().getTblProperties().get(TIME_STAMP_FORMAT) != null) {
                  timestampFormat = carbonQuery.getQuery().getTblProperties().get(TIME_STAMP_FORMAT)
                      .replaceFirst("mm", "MM").replace("hh", "HH").replace("sss", "SSS");
                }
                if (carbonQuery.getQuery().getTblProperties().get(DATE_FORMAT) != null) {
                  dateFormat = carbonQuery.getQuery().getTblProperties().get(DATE_FORMAT).replace("mm", "MM");
                }
              }
              // TODO: get delimiter from job meta and add it into option
              sparkSession.sql(carbonQuery.getQuery().getRewrittenSql())
                  .limit(RESULT_LIMIT_NUM)
                  .repartition(RESULT_PARTITION_NUM)
                  .write()
                  .format("csv")
                  .option(TIME_STAMP_FORMAT, timestampFormat)
                  .option("timestampFormat", timestampFormat)
                  .option(DATE_FORMAT, dateFormat)
                  .option("dateFormat", dateFormat)
                  .save(carbonQuery.getObsPath());
              LOGGER.info(
                  "Carbon query job " + jobID + " executed successfully," + " store path: "
                      + carbonQuery.getObsPath());
            } else {
              // if runnable cmd, we only execute it, do not save result.
              if (carbonQuery.getQuery().getOriginPlan() instanceof RunnableCommand) {
                sparkSession.sql(carbonQuery.getQuery().getRewrittenSql());
              } else {
                sparkSession.sql(carbonQuery.getQuery().getRewrittenSql()).collect();
              }
              LOGGER.info(
                  "Carbon runnable job " + jobID + " executed successfully.");
            }

            //Get and update the endtime/status to cache and metastore.
            JobMeta jobMeta = runningJobsCache.getIfPresent(jobID);
            if (jobMeta != null) {
              runningJobsCache.invalidate(jobID);
            } else {
              jobMeta = new JobMeta(jobID, projectId);
            }
            jobMeta = metaClient.setJobFinished(jobMeta);
            finishedJobsCache.put(jobID, jobMeta);
            LOGGER.info("Job finished: " + jobID);
          } catch (Throwable e) {
            LOGGER.error("Job failed: ", e);
            JobMeta jobMeta = runningJobsCache.getIfPresent(jobID);
            if (jobMeta != null) {
              runningJobsCache.invalidate(jobID);
            } else {
              jobMeta = new JobMeta(jobID, projectId);
            }
            String failReason = projectId != null ?
                e.getMessage().replace(projectId, "") : e.getMessage();
            jobMeta.setFailedReason(failReason);
            failedJobsCache.put(jobID, jobMeta);
            metaClient.setJobFailed(jobMeta);
          }
        } else {
          try {
            Thread.sleep(500);
          } catch (Exception e) {
            LOGGER.error(e);
          }
        }
      }
    }
  }

  @Override
  public SqlResult doSyncJob(Query query) {
    Dataset<Row> rows = SparkSQLUtil.ofRows(sparkSession, query.getOriginPlan());
    SqlResult sqlResult = SqlResultUtil.convertRows(rows);
    return sqlResult;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }

  public void setSparkSession(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public JobMetaStoreClient getMetaClient() {
    return metaClient;
  }

  public void setMetaClient(JobMetaStoreClient metaClient) {
    this.metaClient = metaClient;
  }
}
