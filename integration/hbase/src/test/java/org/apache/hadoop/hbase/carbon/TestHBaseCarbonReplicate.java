package org.apache.hadoop.hbase.carbon;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.carbondata.common.exceptions.sql.InvalidLoadOptionException;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.datastore.impl.FileFactory;
import org.apache.carbondata.core.metadata.AbsoluteTableIdentifier;
import org.apache.carbondata.core.metadata.ColumnarFormatVersion;
import org.apache.carbondata.core.metadata.datatype.DataTypes;
import org.apache.carbondata.core.metadata.schema.SchemaReader;
import org.apache.carbondata.core.metadata.schema.table.TableInfo;
import org.apache.carbondata.core.scan.expression.ColumnExpression;
import org.apache.carbondata.core.scan.expression.Expression;
import org.apache.carbondata.core.scan.expression.LiteralExpression;
import org.apache.carbondata.core.scan.expression.conditional.EqualToExpression;
import org.apache.carbondata.core.scan.model.QueryModel;
import org.apache.carbondata.hadoop.CarbonInputSplit;
import org.apache.carbondata.hadoop.CarbonProjection;
import org.apache.carbondata.hadoop.api.CarbonInputFormat;
import org.apache.carbondata.hadoop.api.CarbonTableInputFormat;
import org.apache.carbondata.hadoop.util.CarbonVectorizedRecordReader;
import org.apache.carbondata.sdk.file.CarbonSchemaWriter;
import org.apache.carbondata.sdk.file.Schema;

import junit.framework.TestCase;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.CarbonHbaseMeta;
import org.apache.hadoop.hbase.coprocessor.CarbonReplicationEndpoint;
import org.apache.hadoop.hbase.replication.ReplicationEndpoint;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.apache.hadoop.hbase.wal.WALEdit;
import org.apache.hadoop.hbase.wal.WALKeyImpl;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.task.JobContextImpl;
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl;
import org.junit.Ignore;
import org.junit.Test;

import static org.apache.hadoop.hbase.coprocessor.CarbonMasterObserver.HBASE_MAPPING_DETAILS;
import static org.apache.hadoop.hbase.coprocessor.CarbonMasterObserver.PATH;

public class TestHBaseCarbonReplicate extends TestCase {

  String path = null;

  {
    try {
      path =
          new File(TestHBaseCarbonReplicate.class.getResource("/").getPath() + "../").getCanonicalPath()
              .replaceAll("\\\\", "/");
    } catch (IOException e) {
      assert (false);
    }
  }

  public void createTable(String schemaStr) throws IOException, InvalidLoadOptionException {
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
    CarbonSchemaWriter.writeSchema(tablePath, schema, new Configuration());
  }

  public CarbonReplicationEndpoint createWriter(String schemaStr)
      throws IOException, InvalidLoadOptionException {

    CarbonReplicationEndpoint endpoint = new CarbonReplicationEndpoint() {
      @Override public String getCarbonSchemaStr(TableName tableName) throws IOException {
        return schemaStr;
      }
    };
    Configuration configuration = new Configuration();
    endpoint.init(configuration);
    return endpoint;
  }

  public void dropTable(String path) {
    FileFactory.deleteAllCarbonFilesOfDir(FileFactory.getCarbonFile(path));
  }

  public void addData(int size, int batchSize, long startKey, CarbonReplicationEndpoint endpoint) {
    ReplicationEndpoint.ReplicateContext context = new ReplicationEndpoint.ReplicateContext();
    List<WAL.Entry> entries = new ArrayList<>();
    WALKeyImpl walKey = new WALKeyImpl("region1".getBytes(), TableName.valueOf("testhbase"), 0L);
    int k = 0;
    System.out.println("Loading data with size " + size + " and batch size " + batchSize);
    for (int i = 0; i < size; i++) {
      byte[] row = Bytes.toBytes(k + startKey);
      byte[] key = Bytes.toBytes("ravi" + (k % 100000));
      byte[] completeKey = new byte[row.length + key.length + 4];
      System.arraycopy(row, 0, completeKey, 0, row.length);
      System.arraycopy(Bytes.toBytes(key.length), 0, completeKey, row.length, 4);
      System.arraycopy(key, 0, completeKey, row.length + 4, key.length);
      Put put = new Put(completeKey);
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("dept"),
          Bytes.toBytes("dept1" + (k % 1000)));
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("city"), Bytes.toBytes("city" + (k % 500)));
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes((short) (k % 60)));
      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("salary"), Bytes.toBytes(k * 200D));
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

  public void deleteData(int size, int batchSize, long startKey, long deletePercent,
      CarbonReplicationEndpoint endpoint) {
    ReplicationEndpoint.ReplicateContext context = new ReplicationEndpoint.ReplicateContext();
    List<WAL.Entry> entries = new ArrayList<>();
    WALKeyImpl walKey = new WALKeyImpl("region1".getBytes(), TableName.valueOf("testhbase"), 0L);
    System.out.println("Loading data with size " + size + " and batch size " + batchSize);
    for (int k = 0; k < size; k++) {
//      if (k % deletePercent != 0) {
//        continue;
//      }
      byte[] row = Bytes.toBytes(k + startKey);
      byte[] key = Bytes.toBytes("ravi" + (k % 100000));
      byte[] completeKey = new byte[row.length + key.length + 4];
      System.arraycopy(row, 0, completeKey, 0, row.length);
      System.arraycopy(Bytes.toBytes(key.length), 0, completeKey, row.length, 4);
      System.arraycopy(key, 0, completeKey, row.length + 4, key.length);
      Delete delete = new Delete(completeKey);
      delete.addFamily(Bytes.toBytes("cf"));
      WALEdit edit = new WALEdit();
      edit.add(delete.getFamilyCellMap());
      entries.add(new WAL.Entry(walKey, edit));
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

  @Test public void testLoadDataAndVerifyCount()
      throws IOException, InvalidLoadOptionException, InterruptedException {
    String tablePath = path + "/tableHbase";
    Configuration conf = new Configuration();
    dropTable(tablePath);
    String schemaStr =
        "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\","
            + "\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\","
            + "\"deletestatus\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\","
            + "\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\"," + "\"primary_key_columns\":\"ID,name\","
            + "\"hbase_mapping\":\"key=ID,key=name,cf:dept=dept,cf:city=city,"
            + "cf:age=age,timestamp=timestamp,deletestatus=deletestatus,cf:salary=salary\","
            + "\"path\":\"" + tablePath + "\"  " + ",\"fs.obs.access.key\":\"" +
            "ak" + "\",\"fs.obs.secret.key\":\"" + "sk" + "\",\"fs.obs.endpoint\":\"" + "ep\"" +  "  }}";
    createTable(schemaStr);
    CarbonReplicationEndpoint endpoint = createWriter(schemaStr);
    int size = 1000;
    addData(size, 100, 0, endpoint);
    CarbonProjection carbonProjection = new CarbonProjection();
    carbonProjection.addColumn("ID");
    carbonProjection.addColumn("dept");
    Expression expression = new EqualToExpression(new ColumnExpression("ID", DataTypes.LONG),
        new LiteralExpression(10L, DataTypes.LONG));
    List<RecordReader> carbonReaders = getCarbonReaders(tablePath, conf, carbonProjection, null);
    int i = 0;

    for (RecordReader reader : carbonReaders) {
      while (reader.nextKeyValue()) {
        Object[] row = (Object[]) reader.getCurrentValue();
        i++;
      }
    }
    System.out.println("Size : " + i);
    assert (i == size);
  }

  @Ignore
  @Test public void testLoadDataAndVerifyFilter()
      throws IOException, InvalidLoadOptionException, InterruptedException {
    String tablePath = path + "/tableHbase";
    Configuration conf = new Configuration();
    dropTable(tablePath);
    String schemaStr =
        "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\","
            + "\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\","
            + "\"deletestatus\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\","
            + "\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\"," + "\"primary_key_columns\":\"ID,name\","
            + "\"hbase_mapping\":\"key=ID,key=name,cf:dept=dept,cf:city=city,"
            + "cf:age=age,timestamp=timestamp,deletestatus=deletestatus,cf:salary=salary\","
            + "\"path\":\"" + tablePath + "\"  " + ",\"fs.s3a.access.key\":\"" +
            "ak" + "\",\"fs.obs.secret.key\":\"" + "sk" + "\",\"fs.obs.endpoint\":\"" + "ep\"" +  "  }}";
    createTable(schemaStr);
    CarbonReplicationEndpoint endpoint = createWriter(schemaStr);
    int size = 1000;
    addData(size, 100, 0, endpoint);
    CarbonProjection carbonProjection = new CarbonProjection();
    carbonProjection.addColumn("ID");
    carbonProjection.addColumn("dept");
    Expression expression = new EqualToExpression(new ColumnExpression("ID", DataTypes.LONG),
        new LiteralExpression(10L, DataTypes.LONG));
    List<RecordReader> carbonReaders =
        getCarbonReaders(tablePath, conf, carbonProjection, expression);
    int i = 0;

    for (RecordReader reader : carbonReaders) {
      while (reader.nextKeyValue()) {
        Object[] row = (Object[]) reader.getCurrentValue();
        System.out.println(Arrays.toString(row));
        i++;
      }
    }
    System.out.println("Size : " + i);
    assert (i == 1);
  }

  @Test public void testLoadDataAndDeleteDataVerifyCount()
      throws IOException, InvalidLoadOptionException, InterruptedException {
    String tablePath = path + "/tableHbase";
    Configuration conf = new Configuration();
    dropTable(tablePath);
    String schemaStr =
        "{\"ID\":\"long\",\"name\":\"string\",\"dept\":\"string\",\"city\":\"string\","
            + "\"age\":\"short\",\"salary\":\"double\",\"timestamp\":\"long\","
            + "\"deletestatus\":\"long\",\"tblproperties\":{\"sort_columns\":\"ID\","
            + "\"table_blocksize\":\"256\",\"table_blocklet_size\":\"32\"," + "\"primary_key_columns\":\"ID,name\","
            + "\"hbase_mapping\":\"key=ID,key=name,cf:dept=dept,cf:city=city,"
            + "cf:age=age,timestamp=timestamp,deletestatus=deletestatus,cf:salary=salary\","
            + "\"path\":\"" + tablePath + "\"  " + ",\"fs.obs.access.key\":\"" +
            "ak" + "\",\"fs.obs.secret.key\":\"" + "sk" + "\",\"fs.obs.endpoint\":\"" + "ep\"" +  "  }}";
    createTable(schemaStr);
    CarbonReplicationEndpoint endpoint = createWriter(schemaStr);
    int size = 1000;
    addData(2, 1, 0, endpoint);
    deleteData(1, 1, 0, 10, endpoint);
    CarbonProjection carbonProjection = new CarbonProjection();
    carbonProjection.addColumn("ID");
    carbonProjection.addColumn("dept");
    List<RecordReader> carbonReaders = getCarbonReaders(tablePath, conf, carbonProjection, null);
    int i = 0;

    for (RecordReader reader : carbonReaders) {
      while (reader.nextKeyValue()) {
        Object[] row = (Object[]) reader.getCurrentValue();
        System.out.println(Arrays.toString(row));
        i++;
      }
    }
    System.out.println("Size : " + i);
    assert (i == 1);
  }

  private List<RecordReader> getCarbonReaders(String tablePath, Configuration conf,
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
}
