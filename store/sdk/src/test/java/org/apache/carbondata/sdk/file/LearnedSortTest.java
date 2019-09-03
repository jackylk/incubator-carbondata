/**
 * Copyright  : Huawei Technologies Co., Ltd. Copyright 2019-2100, All rights reserved.
 * Description: <Description>
 * *
 * Change logs:
 * |----date----|----user----|----comments--------------------------------------------
 * | 2019/4/8   | d00216499  | create and implement this class.
 **/

package org.apache.carbondata.sdk.file;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.memory.IntPointerBuffer;
import org.apache.carbondata.core.memory.MemoryBlock;
import org.apache.carbondata.core.memory.UnsafeMemoryManager;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.datatype.MapType;
import org.apache.carbondata.core.metadata.datatype.StructField;
import org.apache.carbondata.core.metadata.schema.table.CarbonTable;
import org.apache.carbondata.core.metadata.schema.table.TableSchema;
import org.apache.carbondata.core.metadata.schema.table.TableSchemaBuilder;
import org.apache.carbondata.core.metadata.schema.table.column.ColumnSchema;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.carbondata.core.util.ReUsableByteArrayDataOutputStream;
import org.apache.carbondata.processing.loading.CarbonDataLoadConfiguration;
import org.apache.carbondata.processing.loading.DataLoadProcessBuilder;
import org.apache.carbondata.processing.loading.model.CarbonLoadModel;
import org.apache.carbondata.processing.loading.model.CarbonLoadModelBuilder;
import org.apache.carbondata.processing.loading.sort.learned.LearnedArrays;
import org.apache.carbondata.processing.loading.sort.learned.LearnedGetArraysValue;
import org.apache.carbondata.processing.loading.sort.unsafe.UnsafeCarbonRowPage;
import org.apache.carbondata.processing.loading.sort.unsafe.comparator.UnsafeRowComparator;
import org.apache.carbondata.processing.loading.sort.unsafe.holder.UnsafeCarbonRow;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.LearnedSort;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.TimSort;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.UnsafeGetSortValue;
import org.apache.carbondata.processing.loading.sort.unsafe.sort.UnsafeIntSortDataFormat;
import org.apache.carbondata.processing.sort.sortdata.NewRowComparator;
import org.apache.carbondata.processing.sort.sortdata.SortParameters;
import org.apache.carbondata.processing.sort.sortdata.TableFieldStat;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.Test;

public class LearnedSortTest {
  private String path;

  private String taskNo;

  private String taskId;

  private long memoryChunkSizeMB;

  private Schema schema;

  private long timestamp;

  private int blockletSize;

  private int blockSize;

  private int localDictionaryThreshold;

  private boolean isLocalDictionaryEnabled =
          Boolean.parseBoolean(CarbonCommonConstants.LOCAL_DICTIONARY_ENABLE_DEFAULT);

  ReUsableByteArrayDataOutputStream outputStream =
          new ReUsableByteArrayDataOutputStream(new ByteArrayOutputStream());

  private String[] invertedIndexColumns = null;

  private Map<String, String> options = null;

  private String[] sortColumns = null;

  @Before public void cleanFile() {
    String path = null;
    try {
      path = new File(LearnedSortTest.class.getResource("/").getPath() + "../").getCanonicalPath()
              .replaceAll("\\\\", "/");
    } catch (IOException e) {
      assert (false);
    }
    CarbonProperties.getInstance()
            .addProperty(CarbonCommonConstants.CARBON_SYSTEM_FOLDER_LOCATION, path);
  }

  private void testLearnedArraysSort(Object[][] data, SortParameters parameters) {
    long startTime = System.currentTimeMillis();

    LearnedArrays.sort(data, new NewRowComparator(parameters.getNoDictionarySortColumn(),
                    parameters.getNoDictDataType()),
            new LearnedGetArraysValue(parameters.getNoDictionarySortColumn(),
                    parameters.getNoDictDataType()));

    long costTime = System.currentTimeMillis() - startTime;
    System.out.println("LearnedArrays.Sort cost time ms is = " + costTime);
  }

  private void testArraysSort(Object[][] data, SortParameters parameters) {
    long startTime = System.currentTimeMillis();

    Arrays.sort(data, new NewRowComparator(parameters.getNoDictionarySortColumn(),
            parameters.getNoDictDataType()));

    long costTime = System.currentTimeMillis() - startTime;
    System.out.println("Arrays.Sort cost time ms is = " + costTime);
  }

  @Test public void testOnHeapSort() throws Exception {
    int maxCount = 10 * 10000;

    this.path = "./myTestFiles";
    this.taskNo = "5";
    this.taskId = "test2";
    this.memoryChunkSizeMB = 64;

    // sort column def
    this.sortColumns = new String[] { "name" };

    // table def
    Field[] table = new Field[2];
    table[0] = new Field("name", DataTypes.INT);
    table[1] = new Field("age", DataTypes.STRING);

    schema = new Schema(table);
    CarbonLoadModel loadModel = buildLoadModel(schema);
    String[] storeLocation = { path };
    CarbonDataLoadConfiguration configuration =
            DataLoadProcessBuilder.createConfiguration(loadModel, storeLocation);

    SortParameters parameters = SortParameters.createSortParameters(configuration);

    // put data to recordHolderArray
    Object[][] recordHolderArray = new Object[maxCount][];
    Random r = new Random();
    for (int i = 0; i < maxCount; i++) {
      int num = r.nextInt(maxCount);

      Object[] row = { num, (num + "").getBytes() };

      recordHolderArray[i] = row;
    }

    // Arrays test
//    testArraysSort(recordHolderArray, parameters);

    // LearnedArrays sort test
    testLearnedArraysSort(recordHolderArray, parameters);
  }

  private void testTimSort(UnsafeCarbonRowPage rowPage) {
    long startTime = System.currentTimeMillis();
    TimSort<UnsafeCarbonRow, IntPointerBuffer> timSort =
            new TimSort<>(new UnsafeIntSortDataFormat(rowPage));

    timSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeRowComparator(rowPage));

    long costTime = System.currentTimeMillis() - startTime;

    System.out.println("timsort cost time ms = " + costTime);
  }

  private void testLearnedSort(UnsafeCarbonRowPage rowPage) {
    long startTime = System.currentTimeMillis();

    LearnedSort<UnsafeCarbonRow, IntPointerBuffer> learnedSort =
            new LearnedSort<>(new UnsafeIntSortDataFormat(rowPage));

    learnedSort.sort(rowPage.getBuffer(), 0, rowPage.getBuffer().getActualSize(),
            new UnsafeGetSortValue(rowPage), new UnsafeRowComparator(rowPage));

    long costTime = System.currentTimeMillis() - startTime;

    System.out.println("learnedSort cost time ms is = " + costTime);
  }

  @Test public void testOffHeapSort() throws Exception {
    int maxCount = 10 * 10000;

    this.path = "./myTestFiles";
    this.taskNo = "5";
    this.taskId = "test1";
    this.memoryChunkSizeMB = 64;

    // sort column def
    this.sortColumns = new String[] { "name" };

    // table def
    Field[] table = new Field[2];
    table[0] = new Field("name", DataTypes.INT);
    table[1] = new Field("age", DataTypes.STRING);

    // create rowPage
    UnsafeCarbonRowPage rowPage = createUnsafeCarbonRowPage(table);

    // put data to rowPage
    // data size must be <= memoryChunkSizeMB
    Random r = new Random();
    for (int i = 0; i < maxCount; i++) {
      int num = r.nextInt(maxCount);

      String name = "name" + num;
      Object[] row = { num, (num + "").getBytes() };

      rowPage.addRow(row, outputStream);
    }

    // timsort test
    //    testTimSort(rowPage);

    // learnedsort test
    testLearnedSort(rowPage);
  }

  private UnsafeCarbonRowPage createUnsafeCarbonRowPage(Field[] fields) throws Exception {
    FileUtils.deleteDirectory(new File(path));

    schema = new Schema(fields);
    CarbonLoadModel loadModel = buildLoadModel(schema);

    String[] storeLocation = { path };

    CarbonDataLoadConfiguration configuration =
            DataLoadProcessBuilder.createConfiguration(loadModel, storeLocation);

    SortParameters sortParameters = SortParameters.createSortParameters(configuration);

    TableFieldStat tableFieldStat = new TableFieldStat(sortParameters);

    long inMemoryChunkSize = memoryChunkSizeMB * 1024L * 1024L;

    MemoryBlock baseBlock =
            UnsafeMemoryManager.allocateMemoryWithRetry(this.taskId, inMemoryChunkSize);

    return new UnsafeCarbonRowPage(tableFieldStat, baseBlock, taskId, false);
  }

  private CarbonLoadModel buildLoadModel(Schema carbonSchema)
          throws IOException, InvalidLoadOptionException {
    timestamp = System.nanoTime();
    // validate long_string_column
    Set<String> longStringColumns = new HashSet<>();
    if (options != null && options.get(CarbonCommonConstants.LONG_STRING_COLUMNS) != null) {
      String[] specifiedLongStrings =
              options.get(CarbonCommonConstants.LONG_STRING_COLUMNS).toLowerCase().split(",");
      for (String str : specifiedLongStrings) {
        longStringColumns.add(str.trim());
      }
      validateLongStringColumns(carbonSchema, longStringColumns);
    }

    // for the longstring field, change the datatype from string to varchar
    this.schema = updateSchemaFields(carbonSchema, longStringColumns);
    if (sortColumns != null && sortColumns.length != 0) {
      if (options == null || options.get("sort_scope") == null) {
        // If sort_columns are specified and sort_scope is not specified,
        // change sort scope to local_sort as now by default sort scope is no_sort.
        if (CarbonProperties.getInstance().getProperty(CarbonCommonConstants.LOAD_SORT_SCOPE)
                == null) {
          if (options == null) {
            options = new HashMap<>();
          }
          options.put("sort_scope", "local_sort");
        }
      }
    }
    // build CarbonTable using schema
    CarbonTable table = buildCarbonTable();
    // build LoadModel
    return buildLoadModel(table, timestamp, taskNo, options);
  }

  private void validateLongStringColumns(Schema carbonSchema, Set<String> longStringColumns) {
    // long string columns must be string or varchar type
    for (Field field : carbonSchema.getFields()) {
      if (longStringColumns.contains(field.getFieldName().toLowerCase()) && (
              (field.getDataType() != DataTypes.STRING) && field.getDataType() != DataTypes.VARCHAR)) {
        throw new RuntimeException(
                "long string column : " + field.getFieldName() + "is not supported for data type: "
                        + field.getDataType());
      }
    }
    // long string columns must not be present in sort columns
    if (sortColumns != null) {
      for (String col : sortColumns) {
        // already will be in lower case
        if (longStringColumns.contains(col)) {
          throw new RuntimeException(
                  "long string column : " + col + "must not be present in sort columns");
        }
      }
    }
  }

  private CarbonTable buildCarbonTable() {
    TableSchemaBuilder tableSchemaBuilder = TableSchema.builder();
    if (blockSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockSize(blockSize);
    }

    if (blockletSize > 0) {
      tableSchemaBuilder = tableSchemaBuilder.blockletSize(blockletSize);
    }
    tableSchemaBuilder.enableLocalDictionary(isLocalDictionaryEnabled);
    tableSchemaBuilder.localDictionaryThreshold(localDictionaryThreshold);
    List<String> sortColumnsList = new ArrayList<>();
    if (sortColumns == null) {
      // If sort columns are not specified, default set all dimensions to sort column.
      // When dimensions are default set to sort column,
      // Inverted index will be supported by default for sort columns.
      //Null check for field to handle hole in field[] ex.
      //  user passed size 4 but supplied only 2 fileds
      for (Field field : schema.getFields()) {
        if (null != field) {
          if (field.getDataType() == DataTypes.STRING || field.getDataType() == DataTypes.DATE
                  || field.getDataType() == DataTypes.TIMESTAMP) {
            sortColumnsList.add(field.getFieldName());
          }
        }
      }
      sortColumns = new String[sortColumnsList.size()];
      sortColumns = sortColumnsList.toArray(sortColumns);
    } else {
      sortColumnsList = Arrays.asList(sortColumns);
    }
    ColumnSchema[] sortColumnsSchemaList = new ColumnSchema[sortColumnsList.size()];
    List<String> invertedIdxColumnsList = new ArrayList<>();
    if (null != invertedIndexColumns) {
      invertedIdxColumnsList = Arrays.asList(invertedIndexColumns);
    }
    Field[] fields = schema.getFields();
    buildTableSchema(fields, tableSchemaBuilder, sortColumnsList, sortColumnsSchemaList,
            invertedIdxColumnsList);

    tableSchemaBuilder.setSortColumns(Arrays.asList(sortColumnsSchemaList));
    String tableName;
    String dbName;
    dbName = "";
    tableName = "_tempTable_" + String.valueOf(timestamp);
    TableSchema schema = tableSchemaBuilder.build();
    schema.setTableName(tableName);
    CarbonTable table =
            CarbonTable.builder().tableName(schema.getTableName()).databaseName(dbName).tablePath(path)
                    .tableSchema(schema).isTransactionalTable(false).build();
    return table;
  }

  private void buildTableSchema(Field[] fields, TableSchemaBuilder tableSchemaBuilder,
                                List<String> sortColumnsList, ColumnSchema[] sortColumnsSchemaList,
                                List<String> invertedIdxColumnsList) {
    Set<String> uniqueFields = new HashSet<>();
    // a counter which will be used in case of complex array type. This valIndex will be assigned
    // to child of complex array type in the order val1, val2 so that each array type child is
    // differentiated to any level
    AtomicInteger valIndex = new AtomicInteger(0);
    // Check if any of the columns specified in sort columns are missing from schema.
    for (String sortColumn : sortColumnsList) {
      boolean exists = false;
      for (Field field : fields) {
        if (field.getFieldName().equalsIgnoreCase(sortColumn)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        throw new RuntimeException(
                "column: " + sortColumn + " specified in sort columns does not exist in schema");
      }
    }
    // Check if any of the columns specified in inverted index are missing from schema.
    for (String invertedIdxColumn : invertedIdxColumnsList) {
      boolean exists = false;
      for (Field field : fields) {
        if (field.getFieldName().equalsIgnoreCase(invertedIdxColumn)) {
          exists = true;
          break;
        }
      }
      if (!exists) {
        throw new RuntimeException("column: " + invertedIdxColumn
                + " specified in inverted index columns does not exist in schema");
      }
    }
    int i = 0;
    for (Field field : fields) {
      if (null != field) {
        if (!uniqueFields.add(field.getFieldName())) {
          throw new RuntimeException(
                  "Duplicate column " + field.getFieldName() + " found in table schema");
        }
        int isSortColumn = sortColumnsList.indexOf(field.getFieldName());
        int isInvertedIdxColumn = invertedIdxColumnsList.indexOf(field.getFieldName());
        if (isSortColumn > -1) {
          // unsupported types for ("array", "struct", "double", "float", "decimal")
          if (field.getDataType() == DataTypes.DOUBLE || field.getDataType() == DataTypes.FLOAT
                  || DataTypes.isDecimal(field.getDataType()) || field.getDataType().isComplexType()
                  || field.getDataType() == DataTypes.VARCHAR) {
            String errorMsg =
                    "sort columns not supported for array, struct, map, double, float, decimal,"
                            + "varchar";
            throw new RuntimeException(errorMsg);
          }
        }
        if (field.getChildren() != null && field.getChildren().size() > 0) {
          if (field.getDataType().getName().equalsIgnoreCase("ARRAY")) {
            // Loop through the inner columns and for a StructData
            DataType complexType =
                    DataTypes.createArrayType(field.getChildren().get(0).getDataType());
            tableSchemaBuilder
                    .addColumn(new StructField(field.getFieldName(), complexType), valIndex, false,
                            isInvertedIdxColumn > -1);
          } else if (field.getDataType().getName().equalsIgnoreCase("STRUCT")) {
            // Loop through the inner columns and for a StructData
            List<StructField> structFieldsArray =
                    new ArrayList<StructField>(field.getChildren().size());
            for (StructField childFld : field.getChildren()) {
              structFieldsArray
                      .add(new StructField(childFld.getFieldName(), childFld.getDataType()));
            }
            DataType complexType = DataTypes.createStructType(structFieldsArray);
            tableSchemaBuilder
                    .addColumn(new StructField(field.getFieldName(), complexType), valIndex, false,
                            isInvertedIdxColumn > -1);
          } else if (field.getDataType().getName().equalsIgnoreCase("MAP")) {
            // Loop through the inner columns for MapType
            DataType mapType = DataTypes.createMapType(((MapType) field.getDataType()).getKeyType(),
                    field.getChildren().get(0).getDataType());
            tableSchemaBuilder
                    .addColumn(new StructField(field.getFieldName(), mapType), valIndex, false,
                            isInvertedIdxColumn > -1);
          }
        } else {
          ColumnSchema columnSchema = tableSchemaBuilder
                  .addColumn(new StructField(field.getFieldName(), field.getDataType()), valIndex,
                          isSortColumn > -1, isInvertedIdxColumn > -1);
          if (isSortColumn > -1) {
            columnSchema.setSortColumn(true);
            sortColumnsSchemaList[isSortColumn] = columnSchema;
          }
        }
      }
    }
  }

  private CarbonLoadModel buildLoadModel(CarbonTable table, long timestamp, String taskNo,
                                         Map<String, String> options) throws InvalidLoadOptionException, IOException {
    if (options == null) {
      options = new HashMap<>();
    }
    CarbonLoadModelBuilder builder = new CarbonLoadModelBuilder(table);
    CarbonLoadModel build = builder.build(options, timestamp, taskNo);
    return build;
  }

  private Schema updateSchemaFields(Schema schema, Set<String> longStringColumns) {
    if (schema == null) {
      return null;
    }
    Field[] fields = schema.getFields();
    for (int i = 0; i < fields.length; i++) {
      if (fields[i] != null) {
        if (longStringColumns != null) {
          /* Also update the string type to varchar */
          if (longStringColumns.contains(fields[i].getFieldName())) {
            fields[i].updateDataTypeToVarchar();
          }
        }
      }
    }
    return new Schema(fields);
  }
}
