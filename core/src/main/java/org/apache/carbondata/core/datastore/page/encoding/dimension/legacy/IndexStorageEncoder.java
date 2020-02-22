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

package org.apache.carbondata.core.datastore.page.encoding.dimension.legacy;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.columnar.BlockIndexerStorage;
import org.apache.carbondata.core.datastore.page.ColumnPage;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoder;
import org.apache.carbondata.core.datastore.page.encoding.ColumnPageEncoderMeta;
import org.apache.carbondata.format.DataChunk2;
import org.apache.carbondata.format.SortState;

public abstract class IndexStorageEncoder extends ColumnPageEncoder {
  BlockIndexerStorage indexStorage;
  ByteBuffer compressedDataPage;

  abstract void encodeIndexStorage(ColumnPage inputPage);

  @Override
  protected ByteBuffer encodeData(ColumnPage input) throws IOException {
    encodeIndexStorage(input);
//    ByteArrayOutputStream stream = new ByteArrayOutputStream();
//    DataOutputStream out = new DataOutputStream(stream);
//    out.write(compressedDataPage.array(), 0, compressedDataPage.position());
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      compressedDataPage.putInt(indexStorage.getRowIdPageLengthInBytes());
      short[] rowIdPage = indexStorage.getRowIdPage();
      for (short rowId : rowIdPage) {
        compressedDataPage.putShort(rowId);
      }
      if (indexStorage.getRowIdRlePageLengthInBytes() > 0) {
        short[] rowIdRlePage = indexStorage.getRowIdRlePage();
        for (short rowIdRle : rowIdRlePage) {
          compressedDataPage.putShort(rowIdRle);
        }
      }
    }
    if (indexStorage.getDataRlePageLengthInBytes() > 0) {
      short[] dataRlePage = indexStorage.getDataRlePage();
      for (short dataRle : dataRlePage) {
        compressedDataPage.putShort(dataRle);
      }
    }
//    byte[] result = stream.toByteArray();
//    stream.close();
    return compressedDataPage;
  }

  @Override
  protected ColumnPageEncoderMeta getEncoderMeta(ColumnPage inputPage) {
    return null;
  }

  @Override
  protected void fillLegacyFields(DataChunk2 dataChunk) {
    SortState sort = (indexStorage.getRowIdPageLengthInBytes() > 0) ?
        SortState.SORT_EXPLICIT : SortState.SORT_NATIVE;
    dataChunk.setSort_state(sort);
    if (indexStorage.getRowIdPageLengthInBytes() > 0) {
      int rowIdPageLength = CarbonCommonConstants.INT_SIZE_IN_BYTE +
          indexStorage.getRowIdPageLengthInBytes() +
          indexStorage.getRowIdRlePageLengthInBytes();
      dataChunk.setRowid_page_length(rowIdPageLength);
    }
    if (indexStorage.getDataRlePageLengthInBytes() > 0) {
      dataChunk.setRle_page_length(indexStorage.getDataRlePageLengthInBytes());
    }
    dataChunk.setData_page_length(compressedDataPage.limit() - compressedDataPage.position());
  }
}