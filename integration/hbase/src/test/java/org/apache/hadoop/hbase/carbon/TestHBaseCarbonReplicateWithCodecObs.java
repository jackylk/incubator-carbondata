package org.apache.hadoop.hbase.carbon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataType;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.expression.conditional.NotEqualsExpression;
import org.apache.carbondata.core.scan.expression.logical.AndExpression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.core.util.ThreadLocalSessionInfo;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonVectorizedRecordReader;
import org.apache.carbondata.sdk.file.CarbonSchemaWriter;
import org.apache.carbondata.sdk.file.Schema;

import com.huawei.cloudtable.leo.HBaseIdentifier;
import com.huawei.cloudtable.leo.HBaseRowKeyCodecType;
import com.huawei.cloudtable.leo.HBaseTableColumnReference;
import com.huawei.cloudtable.leo.HBaseTableReference;
import com.huawei.cloudtable.leo.HBaseValueCodecManager;
import com.huawei.cloudtable.leo.Identifier;
import com.huawei.cloudtable.leo.ValueTypeManager;
import com.huawei.cloudtable.leo.expression.Constant;
import com.huawei.cloudtable.leo.expression.Evaluation;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CarbonHbaseMeta;
import org.apache.hadoop.hbase.coprocessor.CarbonReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;

import static org.apache.hadoop.hbase.coprocessor.CarbonMasterObserver.HBASE_MAPPING_DETAILS;
import static org.apache.hadoop.hbase.coprocessor.CarbonMasterObserver.PATH;

public class TestHBaseCarbonReplicateWithCodecObs {
  private static String path = "";

  public static void main(String[] args) throws Exception {
    if (args.length != 1 && args.length != 6) {
      throw new Exception("Pls use any of these command for main method: "
          + "1. local "
          + "2. obs <ak> <sk> <endpoint> <obs store path> <carbon config path>");
    }
    switch (args[0].toLowerCase()) {
      case "local":
        path = new File(TestHBaseCarbonReplicateWithCodecObs.class.getResource("/")
            .getPath() + "../").getCanonicalPath().replaceAll("\\\\", "/");
        testLoadDataWithCodec("", "", "", "");
        break;
      case "obs":
        path = args[4];
        testLoadDataWithCodec(args[1], args[2], args[3], args[5]);
        break;
      default:
        throw new Exception("Unsupported is this test: " + args[0]);
    }

    System.exit(0);
  }

  private static void createTable(String schemaStr, Configuration config) throws
      IOException, InvalidLoadOptionException {
    // Get the schema from table desc and convert it into JSON format
    Map<String, String> tblproperties = new HashMap<>();
    Schema schema = CarbonSchemaWriter.convertToSchemaFromJSON(schemaStr, tblproperties);
    CarbonHbaseMeta hbaseMeta = new CarbonHbaseMeta(schema, tblproperties);
    // Carbon writer only allow predefined properties, remove the hbase mapping
    schema.getProperties().remove(HBASE_MAPPING_DETAILS);
    String tablePath = schema.getProperties().get(PATH);
    schema.getProperties().remove(PATH);
    schema.getProperties()
        .put(CarbonCommonConstants.PRIMARY_KEY_COLUMNS, hbaseMeta.getPrimaryKeyColumns());
    if (tablePath == null) {
      throw new IOException("Path cannot be null, please specify in carbonschema");
    }

    // Write the table schema into configured sink path
    CarbonSchemaWriter.writeSchema(tablePath, schema, config);
  }

  public static CarbonReplicationEndpoint createWriter(String schemaStr,
      String ak, String sk, String endPoint, String carbonConfPath) throws IOException {

    CarbonReplicationEndpoint endpoint = new CarbonReplicationEndpoint() {
      @Override public String getCarbonSchemaStr(TableName tableName) throws IOException {
        return schemaStr;
      }
    };
    Configuration configuration = getTestConfig(ak, sk, endPoint, carbonConfPath);
    endpoint.init(configuration);
    return endpoint;
  }

  public static void dropTable(String path) {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(path));
  }

  public static Put buildPut(int i) {
    final HBaseTableReference tableReference = getTestTableReference();
    final TableDefinition.Column<?>[] columns = new TableDefinition.Column<?>[]{
        tableReference.getColumn(Identifier.of("ID")),
        tableReference.getColumn(Identifier.of("name")),
        tableReference.getColumn(Identifier.of("dept")),
        tableReference.getColumn(Identifier.of("city")),
        tableReference.getColumn(Identifier.of("age")),
        tableReference.getColumn(Identifier.of("salary"))
    };
    final Evaluation<?>[] columnValues = new Evaluation<?>[]{
        new Constant<>(Long.class, 1L),
        new Constant<>(String.class, "zhangshunyu" + i),
        new Constant<>(String.class, "cloud bu" + i),
        new Constant<>(String.class, "shenzhen" + i),
        new Constant<>(Short.class, (short)29),
        new Constant<>(Double.class, 0.12345)
    };
    return HBasePutBuilderForTest.build(tableReference, columns, columnValues);
  }

  private static final HBaseIdentifier DEFAULT_FAMILY = HBaseIdentifier.of("cf");

  private static HBaseTableReference getTestTableReference() {
    final TableDefinition.Builder tableDefinitionBuilder = new TableDefinition.Builder(Identifier.of("testhbase"));
    final TableDefinition.Column<?> IDColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("ID"),
        ValueTypeManager.BUILD_IN.getValueType(Long.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> nameColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("name"),
        ValueTypeManager.BUILD_IN.getValueType(String.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> deptColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("dept"),
        ValueTypeManager.BUILD_IN.getValueType(String.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> cityColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("city"),
        ValueTypeManager.BUILD_IN.getValueType(String.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> ageColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("age"),
        ValueTypeManager.BUILD_IN.getValueType(Short.class),
        false,
        null,
        null
    );
    final TableDefinition.Column<?> salaryColumn = tableDefinitionBuilder.addColumn(
        Identifier.of("salary"),
        ValueTypeManager.BUILD_IN.getValueType(Double.class),
        false,
        null,
        null
    );

    tableDefinitionBuilder.setPrimaryKey(Identifier.of("PK"), Identifier.of("ID"), Identifier.of("name"));
    final TableDefinition tableDefinition = tableDefinitionBuilder.build();
    final List<HBaseTableColumnReference<?>> tableColumnReferences = new ArrayList<>(tableDefinition.getColumnCount());
    tableColumnReferences.add(new HBaseTableColumnReference(IDColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(IDColumn.getValueClass())));
    tableColumnReferences.add(new HBaseTableColumnReference(nameColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(nameColumn.getValueClass())));
    tableColumnReferences.add(new HBaseTableColumnReference(deptColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(deptColumn.getValueClass()), DEFAULT_FAMILY, HBaseIdentifier.of(deptColumn.getName().toString())));
    tableColumnReferences.add(new HBaseTableColumnReference(cityColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(cityColumn.getValueClass()), DEFAULT_FAMILY, HBaseIdentifier.of(cityColumn.getName().toString())));
    tableColumnReferences.add(new HBaseTableColumnReference(ageColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(ageColumn.getValueClass()), DEFAULT_FAMILY, HBaseIdentifier.of(ageColumn.getName().toString())));
    tableColumnReferences.add(new HBaseTableColumnReference(salaryColumn,
        HBaseValueCodecManager.BUILD_IN.getValueCodec(salaryColumn.getValueClass()), DEFAULT_FAMILY, HBaseIdentifier.of(salaryColumn.getName().toString())));

    HBaseTableColumnReference rowMarkColumn = tableColumnReferences.get(2);
    return new HBaseTableReference(
        "",
        HBaseIdentifier.of("testhbase"),
        tableDefinition,
        tableColumnReferences,
        HBaseRowKeyCodecType.DELIMITER,
        rowMarkColumn.getFamily(),
        rowMarkColumn.getQualifier()
    );
  }

  public static void addData(int size, int batchSize, long startKey, CarbonReplicationEndpoint endpoint) {
    ReplicationEndpoint.ReplicateContext context = new ReplicationEndpoint.ReplicateContext();
    List<WAL.Entry> entries = new ArrayList<>();
    Random random = new Random();
    int k = 0;
    System.out.println("Loading data with size " + size + " and batch size " + batchSize);
    for (int i = 0; i < size; i++) {
      WALKeyImpl walKey = new WALKeyImpl(("region" + random.nextInt(10)).getBytes(), TableName.valueOf("testhbase"), 0L);
      Put put = buildPut(i);
      WALEdit edit = new WALEdit();
      edit.add(put.getFamilyCellMap());
      entries.add(new WAL.Entry(walKey, edit));
      k++;
      if (k % batchSize == 0) {
        context = context.setEntries(entries);
        endpoint.replicate(context);
        entries = new ArrayList<>();
      }
    }
    if (entries.size() > 0) {
      context = context.setEntries(entries);
      endpoint.replicate(context);
    }
  }

  private static void testLoadDataWithCodec(String ak, String sk, String endPoint,
      String carbonConfigPath)
      throws IOException, InvalidLoadOptionException, InterruptedException {
    String tablePath = path + "/tableHbase";
    Configuration config = getTestConfig(ak, sk, endPoint, carbonConfigPath);
    ThreadLocalSessionInfo.setConfigurationToCurrentThread(config);

    dropTable(tablePath);
    String schemaStr =
        "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\","
            + "\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\","
            + "\"deletestatus\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\","
            + "\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\"," + "\"primary_key\":\"ID,name\","
            + "\"hbase_mapping\":\"key=ID,key=name,cf:dept=dept,cf:city=city,"
            + "cf:age=age,timestamp=timestamp,deletestatus=deletestatus,cf:salary=salary\","
            + "\"path\":\"" + tablePath + "\"  "
            + ",\"carbon_table_id\":\"" + "123456789" + "\"  "
            +",\"fs.obs.access.key\":\"" + ak + "\",\"fs.obs.secret.key\":\"" +
            sk + "\",\"fs.obs.endpoint\":\"" + endPoint +
            "\"" +  "  }}";

    createTable(schemaStr, config);
    CarbonReplicationEndpoint endpoint = createWriter(schemaStr, ak, sk, endPoint, carbonConfigPath);
    addData(20000, 2000, 0, endpoint);
    //    deleteData(1, 1, 0, 10, endpoint);
    CarbonProjection carbonProjection = new CarbonProjection();
    carbonProjection.addColumn("ID");
    carbonProjection.addColumn("dept");
    Expression left = new NotEqualsExpression(new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression(null, DataTypes.STRING));
    Expression right = new EqualToExpression(new ColumnExpression("name", DataTypes.STRING),
        new LiteralExpression("zhangshunyu198", DataTypes.STRING));
    Expression expression = new AndExpression(left, right);
    List<RecordReader> carbonReaders = getCarbonReaders(tablePath, config, carbonProjection, expression);
    int i = 0;

    for (RecordReader reader : carbonReaders) {
      while (reader.nextKeyValue()) {
        Object[] row = (Object[]) reader.getCurrentValue();
        i++;
        System.out.println("Row : " + Arrays.toString(row));
      }
    }
    System.out.println("Size : " + i);
    assert (i == 1);
  }

  private static List<RecordReader> getCarbonReaders(String tablePath, Configuration conf,
      CarbonProjection carbonProjection, Expression expression)
      throws IOException, InterruptedException {
    AbsoluteTableIdentifier identifier = AbsoluteTableIdentifier.from(tablePath, "default", "jj");
    TableInfo tableInfo = SchemaReader.getTableInfo(identifier);
    CarbonInputFormat.setTableInfo(conf, tableInfo);
    CarbonInputFormat.setDatabaseName(conf, tableInfo.getDatabaseName());
    CarbonInputFormat.setTableName(conf, tableInfo.getFactTable().getTableName());

    CarbonInputFormat.setTransactionalTable(conf, tableInfo.isTransactionalTable());
    CarbonTableInputFormat format = new CarbonTableInputFormat<Object>();
    CarbonInputFormat.setTablePath(conf, tablePath);
    CarbonInputFormat.setQuerySegment(conf, identifier);
    CarbonInputFormat.setColumnProjection(conf, carbonProjection);

    CarbonInputFormat.setFilterPredicates(conf, expression);

    List<InputSplit> splits = format.getSplits(new JobContextImpl(conf, new JobID()));

    TaskAttemptContextImpl attempt = new TaskAttemptContextImpl(conf, new TaskAttemptID());
    List<RecordReader> recordReaders = new ArrayList<>();
    for (InputSplit inputSplit : splits) {
      ((CarbonInputSplit) inputSplit).setVersion(ColumnarFormatVersion.R1);
      QueryModel queryModel = format.createQueryModel(inputSplit, attempt);
      CarbonVectorizedRecordReader reader = new CarbonVectorizedRecordReader(queryModel);
      reader.initialize(inputSplit, attempt);
      recordReaders.add(reader);
    }
    return recordReaders;
  }

  private static Configuration getTestConfig(String ak, String sk, String endPoint,
      String carbonConfPath) {
    Configuration config = new Configuration();
    config.set("fs.obs.access.key", ak);
    config.set("fs.obs.secret.key", sk);
    config.set("fs.obs.endpoint", endPoint);
    config.set("spark.hadoop.fs.obs.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    config.set("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.obs.OBSFileSystem");
    config.set("hbase.carbon.config.folder", carbonConfPath);
    return config;
  }
}
