package org.apache.spark.sql.leo.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.logging.LogServiceFactory;
import org.apache.carbondata.core.constants.CarbonCommonConstants;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.huawei.cloud.obs.OBSSparkConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptor;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.TableDescriptor;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.carbondata.execution.datasources.CarbonSparkDataSourceUtil;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.types.StructField;

public class HBaseUtil {
  private static final Logger LOGGER = LogServiceFactory.getLogService(HBaseUtil.class.getName());

  public static final String CARBON_SCHEMA = "CARBON_SCHEMA";
  public static final String CARBON_TIMESTAMP = "timestamp";
  public static final String CARBON_DELETE_STATUS = "deletestatus";
  public static final String HBASE_MAPPING = "hbase_mapping";
  public static final String PATH = "path";
  public static final String TBL_PROPERTIES = "tblproperties";
  public static final String PK_TABLE_CF = "cf";
  public static final String CARBON_TABLE_ID = "carbon_table_id";
  public static final String TABLE_PATH = "tablepath";
  // leader share the same hbase conf
  private static Configuration hbaseConf;

  public static void deleteTable(String db, String table, Boolean ignoreIfExists,
      SparkSession sparkSession) throws IOException {
    Configuration conf = getHBaseConf(sparkSession);

    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      String tableNameWithNs = db + ":" + table;
      TableName tableName = TableName.valueOf(tableNameWithNs);
      Admin admin = conn.getAdmin();
      boolean tableExists = false;
      if (admin.isTableAvailable(tableName)) {
        tableExists = true;
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
      }

      if (tableExists && !ignoreIfExists) {
        //TODO return Table exists Exception
      }
    }

  }

  public static void createTable(String db, String table, Boolean ifNotExistsSet,
      String carbonSchema, SparkSession sparkSession) throws IOException {
    Configuration conf = getHBaseConf(sparkSession);

    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      String tableNameWithNameSpace = db + ":" + table;
      TableName tableName = TableName.valueOf(tableNameWithNameSpace);
      Admin admin = conn.getAdmin();
      boolean tableExists = true;
      if (!admin.isTableAvailable(tableName)) {
        tableExists = false;
        ColumnFamilyDescriptor cfDesc =
            ColumnFamilyDescriptorBuilder.newBuilder(PK_TABLE_CF.getBytes())
                .setCompressionType(Compression.Algorithm.SNAPPY)
                .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                .build();
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(tableName)
            .setColumnFamily(cfDesc)
            .setValue(CARBON_SCHEMA, carbonSchema)
            .setCoprocessor("com.huawei.cloudtable.leo.aggregate.LeoAggregateEndPoint")
            .build();
        admin.createTable(desc);
        if (Boolean.parseBoolean(sparkSession.conf().get("leo.hbase.to.carbon.enable",
            "true"))) {
          // Pls make sure that HBase cluster CarbonMasterObserver has been registered.
          admin.enableTableReplication(tableName);
        }
      }

      if (tableExists && !ifNotExistsSet) {
        //TODO return Table exists Exception
      }
    }

  }

  private static Configuration getHBaseConf(SparkSession sparkSession) {
    if (hbaseConf == null) {
      hbaseConf = HBaseConfiguration.create();
      hbaseConf.set("hbase.zookeeper.property.clientPort",
          sparkSession.conf().get("hbase.zookeeper.property.clientPort", "2181"));
      hbaseConf.set("hbase.zookeeper.quorum", sparkSession.conf().get("hbase.zookeeper.quorum"));
    }
    return hbaseConf;
  }

  public static void createNameSpace(String dbName, Boolean ifNotExists, SparkSession sparkSession)
      throws IOException {
    Configuration conf = getHBaseConf(sparkSession);

    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();
      NamespaceDescriptor nd = NamespaceDescriptor.create(dbName).build();

      boolean nameSpaceExists = true;
      if (!nameSpaceExists(admin, dbName)) {
        nameSpaceExists = false;
        admin.createNamespace(nd);
      }

      if (nameSpaceExists && !ifNotExists) {
        //TODO return DB exists Exception
      }
    }
  }

  public static void deleteNameSpace(String dbName, Boolean ifExists, SparkSession sparkSession)
      throws IOException {
    Configuration conf = getHBaseConf(sparkSession);

    try (Connection conn = ConnectionFactory.createConnection(conf)) {
      Admin admin = conn.getAdmin();

      boolean nameSpaceExists = false;
      if (nameSpaceExists(admin, dbName)) {
        nameSpaceExists = true;
        admin.deleteNamespace(dbName);
      }

      if (!nameSpaceExists && !ifExists) {
      }
    }
  }

  private static boolean nameSpaceExists(Admin admin, String dbName) throws IOException {
    Boolean contains = false;
    NamespaceDescriptor[] nds = admin.listNamespaceDescriptors();
    if (nds == null || nds.length == 0) {
      return contains;
    }

    for (NamespaceDescriptor nd : nds) {
      if (nd.getName().equals(dbName)) {
        contains = true;
        break;
      }
    }

    return contains;

  }

  public static String buildHBaseCarbonSchema(SparkSession sparkSession,
      CatalogTable table) throws Exception {
    String primaryKeyStr = table.properties().get(CarbonCommonConstants.PRIMARY_KEY_COLUMNS).get();
    String[] keysArr = primaryKeyStr.split(",");
    List<String> keysList = new ArrayList<String>();
    StringBuilder keyStrBuilder = new StringBuilder();
    for (String key : keysArr) {
      keysList.add(key.trim());
      keyStrBuilder.append("key=").append(key.trim()).append(",");
    }
    String keyStr = keyStrBuilder.toString();
    StructField[] fields = table.schema().fields();
    // here should use linked map to keep order of schema
    Map<String, Object> carbonSchemaMap = new LinkedHashMap<>();
    StringBuilder normalColsBuilder = new StringBuilder();
    for (StructField field : fields) {
      carbonSchemaMap.put(field.name(),
          CarbonSparkDataSourceUtil.convertSparkToCarbonDataType(field.dataType()).getName());
      if (!keysList.contains(field.name()) && !field.name().equals(CARBON_DELETE_STATUS) && !field
          .name().equals(CARBON_TIMESTAMP)) {
        normalColsBuilder.append(PK_TABLE_CF + ":" + field.name() + "=" + field.name() + ",");
      }
    }
    String normalColumnsMappingStr = normalColsBuilder.toString();
    String hbaseMapping = keyStr + "timestamp=timestamp,deletestatus=deletestatus";
    if (!normalColumnsMappingStr.isEmpty()) {
      hbaseMapping = hbaseMapping + "," + normalColumnsMappingStr
          .substring(0, normalColumnsMappingStr.length() - 1);
    }
    String carbonTablePath = table.storage().properties().get(TABLE_PATH).get();
    Map<String, Object> tblPropertiesMap = new LinkedHashMap<String, Object>();
    tblPropertiesMap.put(PATH, carbonTablePath);
    if (!table.storage().properties().get(CARBON_TABLE_ID).isEmpty()) {
      String carbonTableId = table.storage().properties().get(CARBON_TABLE_ID).get();
      if (carbonTableId != null) {
        tblPropertiesMap.put(CARBON_TABLE_ID, carbonTableId);
      }
    }
    tblPropertiesMap.put(HBASE_MAPPING, hbaseMapping);
    String ak = sparkSession.conf().get(OBSSparkConstants.AK);
    String sk = sparkSession.conf().get(OBSSparkConstants.SK);
    String endpoint = sparkSession.conf().get(OBSSparkConstants.END_POINT);
    tblPropertiesMap.put(OBSSparkConstants.FS_OBS_AK, ak);
    tblPropertiesMap.put(OBSSparkConstants.FS_OBS_SK, sk);
    tblPropertiesMap.put(OBSSparkConstants.FS_OBS_END_POINT, endpoint);
    carbonSchemaMap.put(TBL_PROPERTIES, tblPropertiesMap);

    try {
      ObjectMapper objectMapper = new ObjectMapper();
      String carbonSchemaJson = objectMapper.writeValueAsString(carbonSchemaMap);
      return carbonSchemaJson;
    } catch (Exception e) {
      LOGGER.error("Failed to create carbon schema for hbase table, " + e.getMessage());
      throw e;
    }

  }

}