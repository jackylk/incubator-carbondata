
package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.aggregate.LeoAggregateEndPoint;
import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.value.Date;
import com.huawei.cloudtable.leo.value.Time;
import com.huawei.cloudtable.leo.value.Timestamp;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class CaseHBaseQueryExecutorTest {

  private static final HBaseTestingUtility UTILITY;

  private static HBaseQueryExecutor queryExecutor = null;

  static {
    final Configuration configuration = HBaseConfiguration.create();
    configuration.set("hbase.regionserver.port", "26020");
    final HBaseTestingUtility utility = new HBaseTestingUtility(configuration);
    try {
      utility.startMiniCluster();
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException(e);
    }
    UTILITY = utility;
  }

  @BeforeClass
  public static void beforeClass() throws IOException {
    final NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("test").build();
    try {
      UTILITY.getAdmin().createNamespace(namespaceDescriptor);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    final TableDescriptorBuilder tableDescriptorBuilder =
      TableDescriptorBuilder.newBuilder(TableName.valueOf("test", "test"));
    tableDescriptorBuilder.setCoprocessor(LeoAggregateEndPoint.class.getName());
    tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).build());
    try {
      UTILITY.getAdmin().createTable(tableDescriptorBuilder.build());
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    queryExecutor = new HBaseQueryExecutor(new Configuration(), new TestConnectionFactory(), new TestMetadataProvider());
    queryExecutor.executeUpdate("insert into test values ('Zhang San', 1, 10, 100, 1000, 10000, true, '2019-7-7'," +
      " 1.1, 1.11, 11111, '2019-07-09 19:06:07', '19:06:07')");
    queryExecutor.executeUpdate("insert into test values ('Li Si', 2, 20, 200, 2000, 20000, false, '2019-7-8', " +
      "2.2, 2.22, 22222, '2019-07-09 19:06:08', '19:06:08')");
    queryExecutor.executeUpdate("insert into test values ('Wang Wu', 3, 30, 300, 3000, 30000, true, '2019-7-9', " +
      "3.3, 3.33, 33333, '2019-07-09 19:06:09', '19:06:09')");
  }

  @Test
  public void testCaseSimple() {
    ResultSet resultSet = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' " +
      "ELSE 'it is a senior' END FROM test ");
    checkResult(resultSet, 3);

    ResultSet resultSet1 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' " +
      "ELSE 'it is a senior' END FROM test WHERE name IN ('Zhang San', 'Wang Wu')");
    checkResult(resultSet1, 2);

    ResultSet resultSet2 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' " +
      "ELSE 'it is a senior' END AS describeInfo FROM test ");
    checkResult(resultSet2, 3);

    ResultSet resultSet3 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' " +
      "ELSE 'it is a senior' END AS describeInfo FROM test WHERE name IN ('Zhang San', 'Wang Wu')");
    checkResult(resultSet3, 2);

    ResultSet resultSet4 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' END " +
      "FROM test ");
    checkResult(resultSet4, 3);

    ResultSet resultSet5 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 15 THEN 'it is a teenager' END " +
      "FROM test WHERE score = 1");
    checkResult(resultSet5, 1);
  }

  @Test
  public void testCaseMultipleWhenThen() {
    ResultSet resultSet = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN 'it is a chirlderen' " +
      " WHEN age <=20 THEN 'it is a teenager' ELSE 'it is a senior' END FROM test");
    checkResult(resultSet, 3);

    ResultSet resultSet1 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN 'it is a chirlderen' " +
      " WHEN age <=20 THEN 'it is a teenager' ELSE 'it is a senior' END AS describeInfo FROM test");
    checkResult(resultSet1, 3);

    ResultSet resultSet2 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN 'it is a chirlderen' " +
      " WHEN age <=20 THEN 'it is a teenager' END AS describeInfo FROM test");
    checkResult(resultSet2, 3);
  }

  @Test
  public void testCaseResultDifferentType() {
    ResultSet resultSet = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN 1 + 1 " +
      " WHEN age <=20 THEN 2 + 2 ELSE 3 + 3 END FROM test");
    checkResult(resultSet, 3);

    ResultSet resultSet1 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN -1 " +
      " WHEN age <=20 THEN false ELSE -3 END FROM test");
    checkResult(resultSet1, 3);

    ResultSet resultSet2 = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN -1 " +
      " WHEN age <=20 THEN 'false' ELSE -3 END FROM test");
    checkResult(resultSet2, 3);
  }

  @Test
  public void testCaseOperateNull() {
    ResultSet resultSet = queryExecutor.executeQuery("SELECT *, CASE WHEN age <= 10 THEN null " +
      " WHEN age <=20 THEN null ELSE null END FROM test");
    checkResult(resultSet, 3);
  }

  @Test
  public void testCaseComplex() {
    ResultSet resultSet = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type" +
      " FROM test" +
      " WHERE score > 0");
    checkResult(resultSet, 3);

    ResultSet resultSet1 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type" +
      " FROM test" +
      " WHERE score > 0" +
      " LIMIT 3");
    checkResult(resultSet1, 3);

    ResultSet resultSet2 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type" +
      " FROM test" +
      " WHERE name IN ('Zhang San', 'Li Si', 'Wang Wu')" +
      " GROUP BY score");
    checkResult(resultSet2, 3);

    ResultSet resultSet3 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type" +
      " FROM test" +
      " WHERE name IN ('Zhang San', 'Li Si', 'Wang Wu')" +
      " GROUP BY score" +
      " LIMIT 3");
    checkResult(resultSet3, 3);

    ResultSet resultSet4 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type" +
      " FROM test" +
      " WHERE name IN ('Zhang San', 'Li Si', 'Wang Wu')" +
      " GROUP BY CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END" +
      " LIMIT 3");
    checkResult(resultSet4, 3);

    ResultSet resultSet5 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type," +
      " SUM ((CASE" +
      " WHEN score = 1 THEN 11" +
      " WHEN score = 2 THEN 22" +
      " WHEN score = 3 THEN 33" +
      " ELSE 0" +
      " END) + 10 ) AS scoreSumResult" +
      " FROM test" +
      " WHERE name IN ('Zhang San', 'Li Si', 'Wang Wu')" +
      " GROUP BY CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END" +
      " LIMIT 3");
    checkResult(resultSet5, 3);

    ResultSet resultSet6 = queryExecutor.executeQuery("SELECT CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END AS type," +
      " SUM ((CASE" +
      " WHEN score = 1 THEN 11" +
      " WHEN score = 2 THEN 22" +
      " WHEN score = 3 THEN 33" +
      " ELSE 0" +
      " END) + 10 ) AS scoreSumResult" +
      " FROM test" +
      " WHERE name IN ('Zhang San', 'Li Si', 'Wang Wu')" +
      " GROUP BY CASE" +
      " WHEN age <= 10 THEN 'children'" +
      " WHEN age <=20 THEN 'teenager'" +
      " ELSE 'senior'" +
      " END" +
      " LIMIT 3");
    checkResult(resultSet6, 3);
  }

  public static void checkResult(final java.sql.ResultSet resultSet, int countNum) {
    try {
      try {
        final ResultSetMetaData metadata = resultSet.getMetaData();
        final int columnCount = metadata.getColumnCount();
        int count = 0;
        while (resultSet.next()) {
          for (int i = 0; i < columnCount; i++) {
            System.out.print(resultSet.getObject(i + 1));
            System.out.print('\t');
          }
          System.out.println();
          count++;
        }
        Assert.assertEquals(countNum, count);
      } finally {
        resultSet.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  public static final class TestConnectionFactory extends HBaseConnectionFactory {

    @Override
    public org.apache.hadoop.hbase.client.Connection newConnection(final Configuration configuration) {
      try {
        return UTILITY.getConnection();
      } catch (IOException exception) {
        exception.printStackTrace();
        throw new RuntimeException(exception);
      }
    }

  }

  public static final class TestMetadataProvider extends HBaseMetadataProvider {

    @SuppressWarnings("unchecked")
    @Override
    public HBaseTableReference getTableReference(final String tenantIdentifier, final Identifier schemaName,
                                                 final Identifier tableName) {
      final TableDefinition.Builder tableDefinitionBuilder = new TableDefinition.Builder(tableName);
      tableDefinitionBuilder.addColumn(Identifier.of("NAME"), ValueTypeManager.BUILD_IN.getValueType(String.class),
        false, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("SCORE"), ValueTypeManager.BUILD_IN.getValueType(Byte.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("AGE"), ValueTypeManager.BUILD_IN.getValueType(Short.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("MONEY"), ValueTypeManager.BUILD_IN.getValueType(Integer.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("LONGNUMBER"), ValueTypeManager.BUILD_IN.getValueType(Long.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("BIGNUMBER"), ValueTypeManager.BUILD_IN.getValueType(BigInteger.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("BOOLEAN"), ValueTypeManager.BUILD_IN.getValueType(Boolean.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("DATE"), ValueTypeManager.BUILD_IN.getValueType(Date.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("FLOAT"), ValueTypeManager.BUILD_IN.getValueType(Float.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("DOUBLE"), ValueTypeManager.BUILD_IN.getValueType(Double.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("BigDecimal"), ValueTypeManager.BUILD_IN.getValueType(BigDecimal.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("TIMESTAMP"), ValueTypeManager.BUILD_IN.getValueType(Timestamp.class),
        true, null, null);
      tableDefinitionBuilder.addColumn(Identifier.of("TIME"), ValueTypeManager.BUILD_IN.getValueType(Time.class),
        true, null, null);
      tableDefinitionBuilder.setPrimaryKey(Identifier.of("PK"), Identifier.of("NAME"));

      final TableDefinition tableDefinition = tableDefinitionBuilder.build();

      final List<HBaseTableColumnReference<?>> columnReferenceList = new ArrayList<>();
      for (int i = 0; i < tableDefinition.getColumnCount(); i++) {
        final TableDefinition.Column<?> column = tableDefinition.getColumn(i);
        if (tableDefinition.getPrimaryKey() != null
          && tableDefinition.getPrimaryKey().getColumnIndex(column) != null) {
          columnReferenceList.add(
            new HBaseTableColumnReference(column, HBaseValueCodecManager.BUILD_IN.getValueCodec(column.getValueClass())));
        } else {
          columnReferenceList.add(
            new HBaseTableColumnReference(column, HBaseValueCodecManager.BUILD_IN.getValueCodec(column.getValueClass()),
              HBaseIdentifier.of("F"), HBaseIdentifier.of(column.getName().toString())));
        }
      }

      return new HBaseTableReference(tenantIdentifier, HBaseIdentifier.of(schemaName.toString()), tableDefinition,
        columnReferenceList, HBaseRowKeyCodecType.DELIMITER, HBaseIdentifier.of("F"), HBaseIdentifier.of(" "));
    }

  }

}
