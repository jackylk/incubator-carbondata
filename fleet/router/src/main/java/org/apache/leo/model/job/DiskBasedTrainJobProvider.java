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

package org.apache.leo.model.job;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperationFactory;
import org.apache.carbondata.core.fileoperations.AtomicFileOperations;
import org.apache.carbondata.core.fileoperations.FileWriteOperation;
import org.apache.carbondata.core.locks.CarbonLockFactory;
import org.apache.carbondata.core.locks.ICarbonLock;
import org.apache.carbondata.core.util.CarbonUtil;

import com.google.gson.Gson;
import org.apache.log4j.Logger;

/**
 * It saves/serializes the array of {{@link TrainJobDetail}} to disk in json format.
 * It ensures the data consistance while concurrent write through write lock. It saves the status
 * to the datamapstatus under the system folder.
 */
public class DiskBasedTrainJobProvider implements TrainJobStorageProvider {

  private static final Logger LOG =
      LogServiceFactory.getLogService(
          DiskBasedTrainJobProvider.class.getName());

  private static final String JOB_INFO = ".jobinfo";

  private String location;

  public DiskBasedTrainJobProvider(String location) {
    this.location = location;
  }

  @Override
  public TrainJobDetail[] getAllTrainJobs(String modelName) throws IOException {
    String statusPath = getJobInfoPath(modelName);
    Gson gsonObjectToRead = new Gson();
    DataInputStream dataInputStream = null;
    BufferedReader buffReader = null;
    InputStreamReader inStream = null;
    TrainJobDetail[] trainJobDetails;
    try {
      if (!FileFactory.isFileExist(statusPath)) {
        return new TrainJobDetail[0];
      }
      dataInputStream =
          FileFactory.getDataInputStream(statusPath, FileFactory.getFileType(statusPath));
      inStream = new InputStreamReader(dataInputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET));
      buffReader = new BufferedReader(inStream);
      trainJobDetails = gsonObjectToRead.fromJson(buffReader, TrainJobDetail[].class);
    } catch (IOException e) {
      LOG.error("Failed to read datamap status", e);
      throw e;
    } finally {
      CarbonUtil.closeStreams(buffReader, inStream, dataInputStream);
    }

    // if trainJobDetails is null, return empty array
    if (null == trainJobDetails) {
      return new TrainJobDetail[0];
    }

    return trainJobDetails;
  }

  private String getJobInfoPath(String modelName) {
    return location
          + CarbonCommonConstants.FILE_SEPARATOR + modelName+JOB_INFO;
  }

  /**
   *
   * This method always overwrites the old file.
   * @throws IOException
   */
  @Override
  public void saveTrainJob(String modelName, TrainJobDetail jobDetail)
      throws IOException {
    if (jobDetail == null) {
      // There is nothing to save
      return;
    }
    ICarbonLock modelLock = getModelLock(modelName);
    boolean locked = false;
    try {
      locked = modelLock.lockWithRetries();
      if (locked) {
        LOG.info("Model lock " +modelName+ " has been successfully acquired.");
        TrainJobDetail[] trainJobDetails = getAllTrainJobs(modelName);
        List<TrainJobDetail> trainJobDetailList = Arrays.asList(trainJobDetails);
        trainJobDetailList = new ArrayList<>(trainJobDetailList);
        trainJobDetailList.add(jobDetail);
        writeJobInfoIntoFile(getJobInfoPath(modelName),
            trainJobDetailList.toArray(new TrainJobDetail[trainJobDetailList.size()]));
      } else {
        String errorMsg = "Saving jobinfo is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        if (modelLock.unlock()) {
          LOG.info("Model lock " +modelName+ " has been successfully released");
        } else {
          LOG.error("Not able to release the lock " + modelName);
        }
      }
    }
  }

  /**
   * writes datamap status details
   *
   * @param trainJobDetails
   * @throws IOException
   */
  private static void writeJobInfoIntoFile(String location,
      TrainJobDetail[] trainJobDetails) throws IOException {
    AtomicFileOperations fileWrite = AtomicFileOperationFactory.getAtomicFileOperations(location);
    BufferedWriter brWriter = null;
    DataOutputStream dataOutputStream = null;
    Gson gsonObjectToWrite = new Gson();
    // write the updated data into the datamap status file.
    try {
      dataOutputStream = fileWrite.openForWrite(FileWriteOperation.OVERWRITE);
      brWriter = new BufferedWriter(new OutputStreamWriter(dataOutputStream,
          Charset.forName(CarbonCommonConstants.DEFAULT_CHARSET)));

      String metadataInstance = gsonObjectToWrite.toJson(trainJobDetails);
      brWriter.write(metadataInstance);
    } catch (IOException ioe) {
      LOG.error("Error message: " + ioe.getLocalizedMessage());
      fileWrite.setFailed();
      throw ioe;
    } finally {
      if (null != brWriter) {
        brWriter.flush();
      }
      CarbonUtil.closeStreams(brWriter);
      fileWrite.close();
    }

  }

  @Override
  public void dropTrainJob(String modelName, String trainJobName) throws IOException {
    ICarbonLock modelLock = getModelLock(modelName);
    boolean locked = false;
    try {
      locked = modelLock.lockWithRetries();
      if (locked) {
        LOG.info("Model lock " +modelName+ " has been successfully acquired.");
        TrainJobDetail[] trainJobDetails = getAllTrainJobs(modelName);
        List<TrainJobDetail> trainJobDetailList = Arrays.asList(trainJobDetails);
        trainJobDetailList = new ArrayList<>(trainJobDetailList);
        for (TrainJobDetail jobDetail : trainJobDetailList) {
          if (jobDetail.getJobName().equalsIgnoreCase(trainJobName)) {
            jobDetail.setStatus(TrainJobDetail.Status.DROPPED);
          }
        }
        writeJobInfoIntoFile(getJobInfoPath(modelName),
            trainJobDetailList.toArray(new TrainJobDetail[trainJobDetailList.size()]));
      } else {
        String errorMsg = "Saving jobinfo is failed due to another process taken the lock"
            + " for updating it";
        LOG.error(errorMsg);
        throw new IOException(errorMsg + " Please try after some time.");
      }
    } finally {
      if (locked) {
        if (modelLock.unlock()) {
          LOG.info("Model lock " +modelName+ " has been successfully released");
        } else {
          LOG.error("Not able to release the lock " + modelName);
        }
      }
    }
  }

  @Override public TrainJobDetail getTrainJob(String modelName, String jobName) throws IOException {
    TrainJobDetail[] trainJobs = getAllTrainJobs(modelName);
    for (TrainJobDetail trainJob : trainJobs) {
      if (trainJob.getJobName().equalsIgnoreCase(jobName)) {
        return trainJob;
      }
    }
    return null;
  }

  @Override public void dropModel(String modelName) throws IOException {
    String jobInfoPath = getJobInfoPath(modelName);
    FileFactory.deleteFile(jobInfoPath, FileFactory.getFileType(jobInfoPath));
  }

  private ICarbonLock getModelLock(String modelName) {
    return CarbonLockFactory
        .getSystemLevelCarbonLockObj(location, modelName+".lock");
  }
}
