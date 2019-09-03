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
package org.apache.carbondata.processing.loading.partition.learned;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.datastore.row.CarbonRow;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.SortColumnRangeInfo;
import org.apache.carbondata.processing.loading.BadRecordsLogger;
import org.apache.carbondata.processing.loading.BadRecordsLoggerProvider;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataField;
import org.apache.carbondata.processing.loading.converter.BadRecordLogHolder;
import org.apache.carbondata.processing.loading.converter.FieldConverter;
import org.apache.carbondata.processing.loading.converter.RowConverter;
import org.apache.carbondata.processing.loading.converter.impl.RowConverterImpl;
import org.apache.carbondata.processing.loading.sort.learned.LearnedDistribution;
import org.apache.carbondata.processing.loading.sort.learned.SerializableLearnedDistribution;

import org.apache.log4j.Logger;

public class LearnedDistributionHolder implements DistributionHolder {
  private static final Logger LOGGER =
      LogServiceFactory.getLogService(LearnedDistributionHolder.class.getName());

  //Maximum string length supported
  private static final int SUPPORT_MAX_LENGTH = 50;

  //Maximum of sample sortColumn data
  private static final int MAX_SAMPLE = 1000000;

  //Sample Rate
  private static final double FRACTION = 0.005;

  private static int UNIT_LENGTH_DOUBLE_BYTE = 65536;

  /**
   * Default epsilon for floating point numbers sampled from the random.
   * To guard against errors from taking log(0), a positive epsilon lower bound is applied.
   * A good value for this parameter is at or near the minimum positive floating
   */
  private static final double RNG_EPSILON = 5e-11;

  // the number of sort columns
  private int numberOfNoDictSortColumns;

  // The min length of data
  private int[] minLength;

  // The max length of data
  private int[] maxLength;

  // Number of data null
  private int[] nullNumberData;

  // The learned init dataLength
  private int[] initLength;

  // Learn doubleByte array
  private short[][] learnDoubleByte;

  private SerializableLearnedDistribution[] learnedDistributions;

  private List<RowConverter> converters;

  private List<CarbonRow> samplesList = new ArrayList<>();

  private LearnedRowParserImpl learnedRowParser;

  private SortColumnRangeInfo sortColumnRangeInfo;

  private BadRecordsLogger badRecordLogger;

  private int countForDropping = 0;

  // when fraction < 0.4, used as a divisor
  private double lnq;

  private int recordSampleNumber;

  private int threadSampleNumber;

  private Random random = new Random();

  public LearnedDistributionHolder(CarbonDataLoadConfiguration configuration) {
    // init sampling
    if (configuration.getWritingCoresCount() > 0) {
      this.recordSampleNumber = MAX_SAMPLE / configuration.getWritingCoresCount();
    } else {
      this.recordSampleNumber = MAX_SAMPLE;
    }
    this.threadSampleNumber = recordSampleNumber;
    this.lnq = Math.log1p(-FRACTION);
    // init learnedRowParser && converter
    DataField[] fields = configuration.getDataFields();
    this.learnedRowParser = new LearnedRowParserImpl(fields, configuration);
    this.sortColumnRangeInfo = configuration.getSortColumnRangeInfo();
    this.converters = new ArrayList<>();
    this.badRecordLogger = BadRecordsLoggerProvider.createBadRecordLogger(configuration);
    RowConverter converter = new RowConverterImpl(fields, configuration, badRecordLogger);
    configuration.setCardinalityFinder(converter);
    this.converters.add(converter);
    try {
      converter.initialize();
    } catch (IOException e) {
      LOGGER.error(e);
    }

    // init learned converter
    this.numberOfNoDictSortColumns = configuration.getNumberOfNoDictSortColumns();
    this.minLength = new int[numberOfNoDictSortColumns];
    this.maxLength = new int[numberOfNoDictSortColumns];
    this.nullNumberData = new int[numberOfNoDictSortColumns];
    this.initLength = new int[numberOfNoDictSortColumns];
    this.learnedDistributions = new SerializableLearnedDistribution[numberOfNoDictSortColumns];
    for (int i = 0; i < numberOfNoDictSortColumns; i++) {
      initDataConverterType(i, fields[i]);
    }

    // init learn double Byte
    int[] initDoubleByteLength = new int[numberOfNoDictSortColumns];
    for (int i = 0; i < initLength.length; i++) {
      initDoubleByteLength[i] = (int) Math.ceil((float) initLength[i] / 2);
    }
    this.learnDoubleByte = new short[numberOfNoDictSortColumns][];
    for (int i = 0; i < initDoubleByteLength.length; i++) {
      this.learnDoubleByte[i] = new short[initDoubleByteLength[i] * UNIT_LENGTH_DOUBLE_BYTE];
    }
  }

  private void initDataConverterType(int sortColumnNum, DataField dataField) {
    if (DataTypes.STRING == dataField.getColumn().getDataType()) {
      minLength[sortColumnNum] = SUPPORT_MAX_LENGTH;
      maxLength[sortColumnNum] = 0;
      initLength[sortColumnNum] = SUPPORT_MAX_LENGTH;
    } else if (DataTypes.TIMESTAMP == dataField.getColumn().getDataType()) {
      minLength[sortColumnNum] = maxLength[sortColumnNum] = initLength[sortColumnNum] = 8;
    } else if (DataTypes.DATE == dataField.getColumn().getDataType()) {
      minLength[sortColumnNum] = maxLength[sortColumnNum] = initLength[sortColumnNum] = 4;
    } else {
      minLength[sortColumnNum] = initLength[sortColumnNum] =
          maxLength[sortColumnNum] = dataField.getColumn().getDataType().getSizeInBytes();
    }
    learnedDistributions[sortColumnNum] =
        LearnedDistribution.getDistribution(dataField.getColumn().getDataType());
  }

  /**
   * only convert sort column fields
   */
  private void convertFakeRow(CarbonRow fakeRow) {
    FieldConverter[] fieldConverters = converters.get(0).getFieldConverters();
    BadRecordLogHolder logHolder = new BadRecordLogHolder();
    logHolder.setLogged(false);
    for (int colIdx : sortColumnRangeInfo.getSortColumnIndex()) {
      fieldConverters[colIdx].convert(fakeRow, logHolder);
    }
  }

  @Override public void add(Object[] line) {
    // parse && convert
    CarbonRow fakeCarbonRow = new CarbonRow(learnedRowParser.parseRow(line));
    convertFakeRow(fakeCarbonRow);

    // learned convert
    for (int i = 0; i < numberOfNoDictSortColumns; i++) {
      learnedDistributions[i]
          .learnPartitionDistribution(fakeCarbonRow.getObject(i), learnDoubleByte[i], minLength,
              maxLength, nullNumberData, i);
    }

    // sampling
    if (bernoulliSample()) {
      if (recordSampleNumber != 0) {
        recordSampleNumber--;
        samplesList.add(fakeCarbonRow);
      } else {
        samplesList.set(random.nextInt(threadSampleNumber), fakeCarbonRow);
      }
    }
  }

  @Override public void close() {
    if (null != badRecordLogger) {
      badRecordLogger.closeStreams();
    }

    if (null != converters) {
      for (RowConverter converter : converters) {
        if (null != converter) {
          converter.finish();
        }
      }
    }
  }

  private boolean bernoulliSample() {
    if (countForDropping > 0) {
      countForDropping -= 1;
      return false;
    } else {
      advance();
      return true;
    }
  }

  private void advance() {
    double u = Math.max(random.nextDouble(), RNG_EPSILON);
    countForDropping = (int) (Math.log(u) / lnq);
  }

  public int[] getMinLength() {
    return minLength;
  }

  public int[] getMaxLength() {
    return maxLength;
  }

  public int[] getNullNumberData() {
    return nullNumberData;
  }

  public short[][] getLearnDoubleByte() {
    return learnDoubleByte;
  }

  public List<CarbonRow> getSamplesList() {
    return samplesList;
  }

  public int getSupportMaxLength() {
    return SUPPORT_MAX_LENGTH;
  }

}
