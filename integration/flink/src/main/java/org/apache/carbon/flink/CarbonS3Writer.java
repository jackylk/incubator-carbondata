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
package org.apache.carbon.flink;

import java.io.ByteArrayInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbon.core.metadata.SegmentManager;
import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.exception.CarbonDataWriterException;
import org.apache.carbondata.core.datastore.filesystem.CarbonFile;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.util.CarbonUtil;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

/**
 * The writer which write flink data to S3(OBS) carbon file.
 */
final class CarbonS3Writer extends CarbonWriter {

    private static final Logger LOGGER = LogServiceFactory.getLogService(CarbonS3Writer.class.getCanonicalName());

    CarbonS3Writer(
        final CarbonTable table,
        final org.apache.carbondata.sdk.file.CarbonWriter writer,
        final String writePath,
        final String writePartition,
        final Properties writerProperties,
        final Configuration configuration
    ) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Open writer. " + this.toString());
        }
        this.table = table;
        this.writer = writer;
        this.writePath = writePath;
        this.writePartition = writePartition;
        this.writerProperties = writerProperties;
        this.configuration = configuration;
    }

    private final CarbonTable table;

    private final org.apache.carbondata.sdk.file.CarbonWriter writer;

    private final String writePath;

    private final String writePartition;

    private final Properties writerProperties;

    private final Configuration configuration;

    private final AtomicInteger elementCount = new AtomicInteger(0);

    @Override
    public void addElement(final String element) throws IOException {
        this.writer.write(element);
        this.elementCount.getAndIncrement();
    }

    @Override
    public void flush() throws IOException {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Flush writer. " + this.toString());
        }
        ThreadLocalSessionInfo.setConfigurationToCurrentThread(this.configuration);
        ThreadLocalSessionInfo.getOrCreateCarbonSessionInfo().getNonSerializableExtraInfo().put("carbonConf", this.configuration);
        try {
            final String s3AccessKey = writerProperties.getProperty(CarbonS3Property.ACCESS_KEY);
            final String s3SecretKey = writerProperties.getProperty(CarbonS3Property.SECRET_KEY);
            final String s3Endpoint = writerProperties.getProperty(CarbonS3Property.ENDPOINT);
            final String s3RemotePath = this.writerProperties.getProperty(CarbonS3Property.DATA_REMOTE_PATH);
            if (s3AccessKey == null) {
                throw new IllegalArgumentException("Writer property [" + CarbonS3Property.ACCESS_KEY + "] is not set.");
            }
            if (s3SecretKey == null) {
                throw new IllegalArgumentException("Writer property [" + CarbonS3Property.SECRET_KEY + "] is not set.");
            }
            if (s3Endpoint == null) {
                throw new IllegalArgumentException("Writer property [" + CarbonS3Property.ENDPOINT + "] is not set.");
            }
            if (s3RemotePath == null) {
                throw new IllegalArgumentException("Writer property [" + CarbonS3Property.DATA_REMOTE_PATH + "] is not set.");
            }
            if (!s3RemotePath.startsWith(CarbonCommonConstants.S3A_PREFIX)) {
                throw new IllegalArgumentException("Writer property [" + CarbonS3Property.DATA_REMOTE_PATH + "] is not a s3a path.");
            }
            final String s3RootPath = s3RemotePath + this.table.getDatabaseName() + "/" + this.table.getTableName() + "/";
            final String outputRootPathRemote = s3RootPath + this.writePartition + "/";
            this.writer.close();
            this.uploadSegmentDataFiles(this.writePath + "Fact/Part0/Segment_null/", outputRootPathRemote);
            try {
                this.writeSegmentFile(this.table, outputRootPathRemote);
            } catch (Throwable exception) {
                this.deleteSegmentDataFilesQuietly(outputRootPathRemote);
                throw exception;
            }
        } finally {
            try {
                FileUtils.deleteDirectory(new File(this.writePath));
            } catch (IOException exception) {
                LOGGER.error(exception.getMessage(), exception);
            }
        }
    }

    @Override
    public void finish() {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Close writer. " + this.toString());
        }
        try {
            final File outputRootPathLocal = new File(this.writePath);
            if (outputRootPathLocal.exists()) {
                FileUtils.deleteDirectory(outputRootPathLocal);
            }
        } catch (IOException exception) {
            LOGGER.error(exception.getMessage(), exception);
        }
    }

    /**
     * Upload local segment files to OBS.
     */
    private void uploadSegmentDataFiles(final String localPath, final String remotePath) {
        final File[] files = new File(localPath).listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Upload file[" + file.getAbsoluteFile() + ")] to OBS start.");
            }
            try {
                CarbonUtil.copyCarbonDataFileToCarbonStorePath(file.getAbsolutePath(), remotePath, 1024);
            } catch (CarbonDataWriterException exception) {
                LOGGER.error(exception.getMessage(), exception);
                throw exception;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Upload file[" + file.getAbsoluteFile() + ")] to OBS end.");
            }
        }
    }

    private void deleteSegmentDataFilesQuietly(final String segmentDataPath) {
        try {
            CarbonUtil.deleteFoldersAndFiles(FileFactory.getCarbonFile(segmentDataPath));
        } catch (Throwable exception) {
            LOGGER.error("Fail to delete segment data path [" + segmentDataPath + "].", exception);
        }
    }

    /**
     * Add segment to carbon table. Modify carbon table metadata.
     */
    private void writeSegmentFile(final CarbonTable table, final String segmentLocation) throws IOException {
        final Map<String, String> options = new HashMap<>(2);
        options.put("path", segmentLocation);
        options.put("format", "carbon");
        LOGGER.info("Add segment[" + segmentLocation + "] to table [" + table.getTableName() + "].");
        final CarbonFile segmentStatusFile = SegmentManager.createSegmentFile(table, options);
        try {
            this.writeSegmentSuccessFile(segmentStatusFile);
        } catch (Throwable exception) {
            SegmentManager.deleteSegmentFileQuietly(segmentStatusFile);
            throw exception;
        }
    }

    private void writeSegmentSuccessFile(final CarbonFile segmentStatusFile) throws IOException {
        final String segmentStatusSuccessFilePath = segmentStatusFile.getCanonicalPath() + ".success";
        final DataOutputStream segmentStatusSuccessOutputStream =
            FileFactory.getDataOutputStream(
                segmentStatusSuccessFilePath,
                FileFactory.getFileType(segmentStatusSuccessFilePath),
                CarbonCommonConstants.BYTEBUFFER_SIZE,
                1024
            );
        try {
            IOUtils.copyBytes(
                new ByteArrayInputStream(new byte[0]),
                segmentStatusSuccessOutputStream,
                CarbonCommonConstants.BYTEBUFFER_SIZE
            );
            segmentStatusSuccessOutputStream.flush();
        } finally {
            try {
                CarbonUtil.closeStream(segmentStatusSuccessOutputStream);
            } catch (IOException exception) {
                LOGGER.error(exception.getMessage(), exception);
            }
        }
    }

}
