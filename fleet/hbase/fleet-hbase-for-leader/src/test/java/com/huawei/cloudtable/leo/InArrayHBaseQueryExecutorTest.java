
package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.aggregate.LeoAggregateEndPoint;
import com.huawei.cloudtable.leo.metadata.TableDefinition;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

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
import org.junit.Test;

public class InArrayHBaseQueryExecutorTest {

  private static final HBaseTestingUtility UTILITY;

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

  @Test
  public void test() throws IOException {
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

    final HBaseQueryExecutor queryExecutor =
      new HBaseQueryExecutor(new Configuration(), new TestConnectionFactory(), new TestMetadataProvider());
    queryExecutor.executeUpdate("insert into test values ('Zhang San', 1, 10, 100, 1000, 10000, true, '2019-7-7', 1.1, 1.11, 11111, '2019-07-09 19:06:07', '19:06:07')");
    queryExecutor.executeUpdate("insert into test values ('Li Si', 2, 20, 200, 2000, 20000, false, '2019-7-8', 2.2, 2.22, 22222, '2019-07-09 19:06:08', '19:06:08')");
    queryExecutor.executeUpdate("insert into test values ('Wang Wu', 3, 30, 300, 3000, 30000, true, '2019-7-9', 3.3, 3.33, 33333, '2019-07-09 19:06:09', '19:06:09')");

    ResultSet resultSet = queryExecutor.executeQuery("select * from test where NAME in ('Zhang San', 'Wang Wu')");
    print(resultSet);

    ResultSet resultSet0 = queryExecutor.executeQuery("select * from test where NAME in ('Zhang San', 'Wang Wu') and SCORE < 3");
    print(resultSet0);

    ResultSet resultSet1 = queryExecutor.executeQuery("select * from test where SCORE in (1, 3)");
    print(resultSet1);

    ResultSet resultSet2 = queryExecutor.executeQuery("select * from test where AGE in (10, 30)");
    print(resultSet2);

    ResultSet resultSet3 = queryExecutor.executeQuery("select * from test where MONEY in (100, 300)");
    print(resultSet3);

    ResultSet resultSet4 = queryExecutor.executeQuery("select * from test where LONGNUMBER in (1000, 3000)");
    print(resultSet4);

    ResultSet resultSet5 = queryExecutor.executeQuery("select * from test where BIGNUMBER in (10000, 30000)");
    print(resultSet5);

    ResultSet result1 = queryExecutor.executeQuery("select * from test where BOOLEAN in (true)");
    print(result1);

    ResultSet result3 = queryExecutor.executeQuery("select * from test where DATE in ('2019-7-7', '2019-7-9')");
    print(result3);

    ResultSet result4 = queryExecutor.executeQuery("select * from test where FLOAT in (1.1, 3.3)");
    print(result4);

    ResultSet result5 = queryExecutor.executeQuery("select * from test where DOUBLE in (1.11, 3.33)");
    print(result5);

    ResultSet result6 = queryExecutor.executeQuery("select * from test where BIGDECIMAL in (11111, 33333)");
    print(result6);

    ResultSet result7 = queryExecutor.executeQuery("select * from test where TIMESTAMP in ('2019-07-09 19:06:07', '2019-07-09 19:06:09')");
    print(result7);

    ResultSet result8 = queryExecutor.executeQuery("select * from test where TIME in ('19:06:07', '19:06:09')");
    print(result8);

    ResultSet resultSet6 = queryExecutor.executeQuery("select sum(SCORE) from test " +
      "where NAME in ('Zhang San', 'Wang Wu')");
    List<List<Comparable>> expectedList6 = getExpectedList(new Integer[]{4});
    Assert.assertTrue(expected(resultSet6, expectedList6));

    ResultSet resultSet7 = queryExecutor.executeQuery("select sum(AGE) from test " +
      "where NAME in ('Zhang San', 'Wang Wu')");
    List<List<Comparable>> expectedList7 = getExpectedList(new Integer[]{40});
    Assert.assertTrue(expected(resultSet7, expectedList7));

    ResultSet resultSet8 = queryExecutor.executeQuery("select sum(MONEY) from test " +
      "where NAME in ('Zhang San', 'Wang Wu')");
    List<List<Comparable>> expectedList8 = getExpectedList(new Integer[]{400});
    Assert.assertTrue(expected(resultSet8, expectedList8));

    ResultSet resultSet9 = queryExecutor.executeQuery("select sum(LONGNUMBER) from test " +
      "where NAME in ('Zhang San', 'Wang Wu')");
    List<List<Comparable>> expectedList9 = getExpectedList(new Integer[]{4000});
    Assert.assertTrue(expected(resultSet9, expectedList9));

    ResultSet resultSet10 = queryExecutor.executeQuery("select sum(BIGNUMBER) from test " +
      "where NAME in ('Zhang San', 'Wang Wu')");
    List<List<Comparable>> expectedList10 = getExpectedList(new Integer[]{40000});
    Assert.assertTrue(expected(resultSet10, expectedList10));
  }

  public static void print(final java.sql.ResultSet resultSet) {
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
        System.out.println("Count: " + count);
        Assert.assertEquals(2, count);
      } finally {
        resultSet.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
  }

  private static boolean expected(final java.sql.ResultSet resultSet, List<List<Comparable>> expectedList) {
    try {
      try {
        final ResultSetMetaData metadata = resultSet.getMetaData();
        final int columnCount = metadata.getColumnCount();
        int count = 0;
        while (resultSet.next()) {
          List<?> expectedRow = expectedList.get(count);
          for (int i = 0; i < columnCount; i++) {
            Object realObject = resultSet.getObject(i + 1);
            Object expectedObject = expectedRow.get(i);
            if (realObject instanceof BigInteger) {
              if (((Comparable) realObject).compareTo(expectedObject) != 0) {
                return false;
              }
            }

          }
          count++;
          return true;
        }
        System.out.println("#####Count: " + count);
      } finally {
        resultSet.close();
      }
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }

    return false;
  }

  private List<List<Comparable>> getExpectedList(Integer[]... rows) {
    List<List<Comparable>> expectedList = new ArrayList<>();
    for (Integer[] row : rows) {
      List<Comparable> rowExpectedList = new ArrayList<>();
      for (Integer object : row) {
        rowExpectedList.add(BigInteger.valueOf(object));
      }
      expectedList.add(rowExpectedList);
    }
    return expectedList;
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
