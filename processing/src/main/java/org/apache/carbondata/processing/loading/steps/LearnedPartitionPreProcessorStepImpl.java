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
package org.apache.carbondata.processing.loading.steps;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.carbondata.common.CarbonIterator;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.util.CarbonThreadFactory;
import org.apache.carbondata.processing.loading.AbstractDataLoadProcessorStep;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.exception.CarbonDataLoadingException;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.partition.learned.DistributionHolder;
import org.apache.carbondata.processing.loading.partition.learned.LearnedDistributionHolder;
import org.apache.carbondata.processing.loading.partition.learned.LearnedPartitionAlgorithm;
import org.apache.carbondata.processing.loading.partition.learned.PartitionAlgorithm;
import org.apache.carbondata.processing.loading.row.CarbonRowBatch;

import org.apache.log4j.Logger;

/**
 * It reads data from record reader and sends data to next step.
 */
public class LearnedPartitionPreProcessorStepImpl extends AbstractDataLoadProcessorStep {

  private static final Logger LOGGER =
          LogServiceFactory.getLogService(LearnedPartitionPreProcessorStepImpl.class.getName());

  private CarbonIterator<Object[]>[] inputIterators;

  private ExecutorService readExecutorService;

  private CarbonLoadModel loadModel;

  private PartitionAlgorithm partitionAlgorithm;

  private DistributionHolder[] distributionHolders;

  public LearnedPartitionPreProcessorStepImpl(CarbonDataLoadConfiguration configuration,
                                              CarbonIterator<Object[]>[] inputIterators, CarbonLoadModel loadModel) {
    super(configuration, null);
    this.inputIterators = inputIterators;
    this.loadModel = loadModel;
  }

  @Override public void initialize() throws IOException {
    super.initialize();

    partitionAlgorithm = new LearnedPartitionAlgorithm(configuration);
  }

  @Override public Iterator<CarbonRowBatch>[] execute() {
    long startTime = System.currentTimeMillis();

    // The thread pool for reading data from original input iterators
    readExecutorService = Executors.newCachedThreadPool(new CarbonThreadFactory(
            "LearnedPartitionPreProcessorPool-Reading Files: " + configuration.getTableIdentifier()
                    .getCarbonTableIdentifier().getTableName()));

    distributionHolders = new DistributionHolder[inputIterators.length];
    for (int i = 0; i < inputIterators.length; i++) {
      distributionHolders[i] = new LearnedDistributionHolder(configuration);

      readExecutorService.execute(
              new InMemoryReadingThread(inputIterators[i], distributionHolders[i], rowCounter));
    }
    readExecutorService.shutdown();

    try {
      readExecutorService.awaitTermination(2, TimeUnit.HOURS);
    } catch (InterruptedException e) {
      throw new CarbonDataLoadingException(e);
    }

    long readTime = System.currentTimeMillis();
    LOGGER.info("Learned partition pre processor read input cost time is: " + (readTime - startTime)
            + " ms.");

    // learned the sort columns bounds
    partitionAlgorithm.learn(distributionHolders);

    // set the sort columns bounds str && learned partition model
    loadModel.setLearnedPartitionModel(partitionAlgorithm.getLearnedPartitionModel());
    loadModel.setSortColumnsBoundsStr(partitionAlgorithm.getSortColumnsBoundsStr());

    long partitionTime = System.currentTimeMillis();
    LOGGER.info(
            "Learned partition pre processor partition cost time is: " + (partitionTime - readTime)
                    + " ms.");
    return null;
  }

  @Override public void close() {
    if (!closed) {
      super.close();

      if (null != readExecutorService) {
        readExecutorService.shutdownNow();
      }
      for (CarbonIterator inputIterator : inputIterators) {
        inputIterator.close();
      }

      if (null != distributionHolders) {
        for (DistributionHolder distributionHolder : distributionHolders) {
          if (null != distributionHolder) {
            distributionHolder.close();
          }
        }
      }
    }
  }

  @Override
  protected String getStepName() {
    return "Learned Partition Pre Processor";
  }

  private static class InMemoryReadingThread implements Runnable {
    private CarbonIterator<Object[]> carbonIterator;
    private AtomicLong rowCounter;
    private DistributionHolder distributionHolder;

    InMemoryReadingThread(CarbonIterator<Object[]> carbonIterator,
                          DistributionHolder distributionHolder, AtomicLong rowCounter) {
      this.carbonIterator = carbonIterator;
      this.rowCounter = rowCounter;
      this.distributionHolder = distributionHolder;
    }

    @Override
    public void run() {
      try {
        int selfCounter = 0;
        carbonIterator.initialize();
        while (carbonIterator.hasNext()) {
          distributionHolder.add(carbonIterator.next());
          selfCounter++;
        }
        rowCounter.getAndAdd(selfCounter);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
}
