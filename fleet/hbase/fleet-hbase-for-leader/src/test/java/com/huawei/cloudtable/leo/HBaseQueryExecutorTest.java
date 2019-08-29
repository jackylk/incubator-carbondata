
package com.huawei.cloudtable.leo;

import com.huawei.cloudtable.leo.metadata.TableDefinition;
import com.huawei.cloudtable.leo.value.Timestamp;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.ColumnFamilyDescriptorBuilder;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.AfterClass;
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

public class HBaseQueryExecutorTest {
    private static final HBaseTestingUtility UTILITY;

    private static HBaseQueryExecutor queryExecutor;

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
    public static void init() throws IOException {
        final NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create("test").build();
        try {
            UTILITY.getAdmin().createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(
          TableName.valueOf("test", "test"));
        tableDescriptorBuilder.setCoprocessor("com.huawei.cloudtable.leo.aggregate.LeoAggregateEndPoint");
        tableDescriptorBuilder.setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("f")).build());
        try {
            UTILITY.getAdmin().createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        queryExecutor =
          new HBaseQueryExecutor(new Configuration(), new TestConnectionFactory(), new TestMetadataProvider());
        queryExecutor.executeUpdate("insert into test values ('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 30, 28, '2019-07-01 14:01:23')");
        queryExecutor.executeUpdate("insert into test values ('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 120, 18, '2019-07-01 14:05:23')");
        queryExecutor.executeUpdate("insert into test values ('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 78, 8, '2019-07-01 14:01:24')");
        queryExecutor.executeUpdate("insert into test values ('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host21', 120, 8, '2019-07-01 14:01:25')");
        queryExecutor.executeUpdate("insert into test values ('www.baidu.com', 'CHN-esbeijing-CT1', 'business.host55', 78, 8, '2019-07-01 14:01:24')");
        queryExecutor.executeUpdate("insert into test values ('www.sohu.com', 'CHN-esbeijing-CT1', 'business.host02', 65, 45, '2019-07-01 14:01:28')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 188, 6, '2019-07-01 14:01:30')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 28, 26, '2019-07-01 14:01:31')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 21, 35, '2019-07-01 14:01:32')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 65, 78, '2019-07-01 14:01:33')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 78, 71, '2019-07-01 14:01:34')");
        queryExecutor.executeUpdate("insert into test values ('www.sina.com', 'CHN-esbeijing-CT1', 'business.host21', 96, 45, '2019-07-01 14:01:35')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 524, 145, '2019-07-01 14:01:30')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 452, 42, '2019-07-01 14:01:31')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 654, 431, '2019-07-01 14:01:32')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 788, 49, '2019-07-01 14:01:33')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 788, 49, '2019-07-01 14:01:34')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 788, 49, '2019-07-01 14:01:35')");
        queryExecutor.executeUpdate("insert into test values ('www.huawei.com', 'CHN-esbeijing-CT1', 'business.host21', 788, 49, '2019-07-01 14:01:36')");

    }

    @Test
    public void tesAggregate(){
        java.sql.ResultSet twoSumPlusResultSet = queryExecutor.executeQuery("select sum(hlb_flux) + sum(hlb_succ_req) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23' group by tm");
        List<List<Comparable>> twoSumPlusExpectedList = getExpectedList(new Comparable[]{BigInteger.valueOf(86)},new Comparable[]{BigInteger.valueOf(128)});
        Assert.assertTrue(expected(twoSumPlusResultSet,twoSumPlusExpectedList));

        java.sql.ResultSet twoSumResultSet = queryExecutor.executeQuery("select sum(hlb_flux),sum(hlb_succ_req) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23' group by tm");
        List<List<Comparable>> expectedList = getExpectedList(new Comparable[]{BigInteger.valueOf(78),BigInteger.valueOf(8)},new Comparable[]{BigInteger.valueOf(120),BigInteger.valueOf(8)});
        Assert.assertTrue(expected(twoSumResultSet,expectedList));

        java.sql.ResultSet count1 = queryExecutor.executeQuery("select count(hlb_flux) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'  group by tm");
        List<List<Comparable>> count1ExpectedList = getExpectedList(new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)});
        Assert.assertTrue(expected(count1,count1ExpectedList));

        java.sql.ResultSet twoSumResultSetNoGroupBy =queryExecutor.executeQuery("select sum(hlb_flux),sum(hlb_succ_req) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> twoSumResultSetNoGroupByExpected = getExpectedList(new Comparable[]{BigInteger.valueOf(198),BigInteger.valueOf(16)});
        Assert.assertTrue(expected(twoSumResultSetNoGroupBy,twoSumResultSetNoGroupByExpected));


        java.sql.ResultSet countNoGroupBy =queryExecutor.executeQuery("select count(hlb_flux) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> countNoGroupByExpected = getExpectedList(new Comparable[]{Long.valueOf(2)});
        Assert.assertTrue(expected(countNoGroupBy,countNoGroupByExpected));

        java.sql.ResultSet avgNoGroupBy =queryExecutor.executeQuery("select avg(hlb_flux) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> avgNoGroupByExpected = getExpectedList(new Comparable[]{BigDecimal.valueOf(99.0)});
        Assert.assertTrue(expected(avgNoGroupBy,avgNoGroupByExpected));

        java.sql.ResultSet sumCountResultSet = queryExecutor.executeQuery("select sum(hlb_flux),count(hlb_succ_req) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23' group by tm");
        List<List<Comparable>> sumCountResultSetExpectedList = getExpectedList(new Comparable[]{BigInteger.valueOf(78),Long.valueOf(1)},new Comparable[]{BigInteger.valueOf(120),Long.valueOf(1)});
        Assert.assertTrue(expected(sumCountResultSet,sumCountResultSetExpectedList));

        java.sql.ResultSet sumCountResultSetNoGroupBy = queryExecutor.executeQuery("select sum(hlb_flux),count(hlb_succ_req) from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> sumCountResultSetNoGroupByExpectedList = getExpectedList(new Comparable[]{BigInteger.valueOf(198),Long.valueOf(2)});
        Assert.assertTrue(expected(sumCountResultSetNoGroupBy,sumCountResultSetNoGroupByExpectedList));

        java.sql.ResultSet noNodeNameResultSet = queryExecutor.executeQuery("select sum(hlb_flux),count(hlb_succ_req) from test where dm='www.sina.com' and node_name='business.host21' and tm>= '2019-07-01 14:01:30' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> noNodeNameResultSetExpectedList = getExpectedList(new Comparable[]{BigInteger.valueOf(476),Long.valueOf(6)});
        Assert.assertTrue(expected(noNodeNameResultSet,noNodeNameResultSetExpectedList));

        java.sql.ResultSet maxResultSet = queryExecutor.executeQuery("select max(hlb_flux), min(hlb_succ_req) from test where dm='www.sina.com' and node_name='business.host21' and tm> '2019-07-01 14:01:30' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> maxResultSetExpectedList = getExpectedList(new Comparable[]{Long.valueOf(96), Long.valueOf(26)});
        Assert.assertTrue(expected(maxResultSet,maxResultSetExpectedList));

        java.sql.ResultSet huaweiResultSet = queryExecutor.executeQuery("select count(hlb_flux) from test where dm='www.huawei.com' and node_name='business.host21' and tm> '2019-07-01 14:01:30' and tm< '2019-07-01 14:02:23' group by tm");
        List<List<Comparable>> huaweiResultSetExpectedList = getExpectedList(new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)},new Comparable[]{Long.valueOf(1)});
        Assert.assertTrue(expected(huaweiResultSet,huaweiResultSetExpectedList));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void testUnsupportedExceptionOfPlus(){
        java.sql.ResultSet avgNoGroupByPlusOne =queryExecutor.executeQuery("select avg(hlb_flux)+1 from test where dm='www.baidu.com' and node_name='business.host21' and tm> '2019-07-01 14:01:23' and tm< '2019-07-01 14:02:23'");
        List<List<Comparable>> avgNoGroupByPlusOneExpected = getExpectedList(new Double[]{100.9});
        Assert.assertTrue(expected(avgNoGroupByPlusOne,avgNoGroupByPlusOneExpected));
    }


    @Test
    public void test() {
        // print(queryExecutor.executeQuery("select count(1) from test where dm = 'www.baidu.com'"));
        // print(queryExecutor.executeQuery("select * from test where hlb_succ_req=78"));
        print(queryExecutor.executeQuery("select * from test where dm = 'www.baidu.com'"));
        print(queryExecutor.executeQuery("select * from test where dm = 'www.sohu.com'"));
    }

    @AfterClass
    public static void close() throws IOException {
        UTILITY.getConnection().close();
    }

    private  List<List<Comparable>> getExpectedList(Comparable[]... rows ){
        List<List<Comparable>> expectedList = new ArrayList<>();
        for(Comparable[] row : rows){
            List<Comparable> rowExpectedList = new ArrayList<>();
            for(Comparable object : row){
                rowExpectedList.add(object);
            }
            expectedList.add(rowExpectedList);
        }
        return expectedList;
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
                        if(realObject == null){
                            return false;
                        }
                        Object expectedObject = expectedRow.get(i);
                        if(realObject instanceof Double){
                            BigDecimal real = BigDecimal.valueOf((Double)realObject);
                            if(real.compareTo((BigDecimal)expectedObject) != 0){
                                return false;
                            }
                        } else if(realObject instanceof Float){
                            BigDecimal real = BigDecimal.valueOf((Float)realObject);
                            if(real.compareTo((BigDecimal)expectedObject) != 0){
                                return false;
                            }
                        } else {
                            if(((Comparable) realObject).compareTo(expectedObject) != 0){
                                return false;
                            }
                        }
                    }
                    count++;
                }

                System.out.println("#####Count: " + count);
                if(count != expectedList.size()){
                    return false;
                }
                return true;
            } finally {
                resultSet.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }

    }


    public static void print(final java.sql.ResultSet resultSet) {
        try {
            try {
                final ResultSetMetaData metadata = resultSet.getMetaData();
                final int columnCount = metadata.getColumnCount();
                int count = 0;
                while (resultSet.next()) {
                    for (int i = 0; i < columnCount; i++) {
                        resultSet.getObject(i + 1);
                    }
                    count++;
                }
                System.out.println("Count: " + count);
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
            tableDefinitionBuilder.addColumn(Identifier.of("dm"), ValueTypeManager.BUILD_IN.getValueType(String.class),
              false, null, null);
            tableDefinitionBuilder.addColumn(Identifier.of("sg_type"), ValueTypeManager.BUILD_IN.getValueType(String.class),
              true, null, null);
            tableDefinitionBuilder.addColumn(Identifier.of("node_name"), ValueTypeManager.BUILD_IN.getValueType(String.class),
              true, null, null);
            tableDefinitionBuilder.addColumn(Identifier.of("hlb_flux"), ValueTypeManager.BUILD_IN.getValueType(Long.class),
              true, null, null);
            tableDefinitionBuilder.addColumn(Identifier.of("hlb_succ_req"), ValueTypeManager.BUILD_IN.getValueType(Long.class),
              true, null, null);
            tableDefinitionBuilder.addColumn(Identifier.of("tm"), ValueTypeManager.BUILD_IN.getValueType(Timestamp.class),
              true, null, null);
            tableDefinitionBuilder.setPrimaryKey(Identifier.of("PK"), Identifier.of("dm"),Identifier.of("node_name"),Identifier.of("tm"));

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