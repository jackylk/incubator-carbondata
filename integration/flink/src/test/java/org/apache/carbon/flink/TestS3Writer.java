package org.apache.carbon.flink;

import java.sql.Timestamp;
import java.util.Properties;
import org.apache.carbondata.core.constants.CarbonCommonConstants;
import org.apache.carbondata.core.util.CarbonProperties;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.internal.SQLConf;
import org.apache.spark.sql.test.Spark2TestQueryExecutor;
import org.apache.spark.sql.test.util.QueryTest;

public class TestS3Writer extends QueryTest {

    public void test() {
        final String accessKey = "S3 AK";
        final String secretKey = "S3 SK";
        final String endpoint = "S3 Endpoint";
        final String bucket = "S3 Bucket";
        final String zookeeperURL = "Zookeeper URL";

        final String lockType = CarbonCommonConstants.CARBON_LOCK_TYPE_ZOOKEEPER;
        final String lockPath = "default";
        final String storeLocation = CarbonCommonConstants.S3A_PREFIX + bucket + "/root";

        final String databaseName = "default";
        TestSource.DATA_COUNT.set(0);
        final String tableName = "flink_to_carbon_s3";

        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.LOCK_PATH, "/tmp/locks");
        CarbonProperties.getInstance().addProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation);
        setSparkSessionConfiguration(accessKey, secretKey, endpoint);

        // Initialize test environment.
        sql("drop table if exists " + tableName);
        sql("CREATE TABLE " + tableName + " (stringField string, intField int, shortField short, longField long, doubleField double, boolField boolean, dateField date, timeField timestamp, decimalField decimal(8,2)) STORED AS carbondata tblproperties('sort_scope'='local_sort', 'sort_column'='stringField')");

        // Set up the execution environment.
        final Properties writerProperties = new Properties();
        writerProperties.setProperty(CarbonS3Property.DATA_LOCAL_PATH, System.getProperty("java.io.tmpdir") + "/" + System.currentTimeMillis() + "/");
        writerProperties.setProperty(CarbonS3Property.ACCESS_KEY, accessKey);
        writerProperties.setProperty(CarbonS3Property.SECRET_KEY, secretKey);
        writerProperties.setProperty(CarbonS3Property.ENDPOINT, endpoint);
        writerProperties.setProperty(CarbonS3Property.DATA_REMOTE_PATH, CarbonCommonConstants.S3A_PREFIX + bucket + "/data/");
        writerProperties.setProperty(CarbonCommonConstants.LOCK_TYPE, lockType);
        writerProperties.setProperty(CarbonCommonConstants.LOCK_PATH, lockPath);
        writerProperties.setProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperURL);
        writerProperties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation);
        writerProperties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "10240");

        final Properties carbonProperties = new Properties();
        carbonProperties.setProperty(CarbonCommonConstants.LOCK_TYPE, lockType);
        carbonProperties.setProperty(CarbonCommonConstants.LOCK_PATH, lockPath);
        carbonProperties.setProperty(CarbonCommonConstants.ZOOKEEPER_URL, zookeeperURL);
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT, CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT);
        carbonProperties.setProperty(CarbonCommonConstants.CARBON_DATE_FORMAT, CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT);
        carbonProperties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation);
        carbonProperties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "10240");

        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(5);
        environment.enableCheckpointing(60000);
        environment.setRestartStrategy(RestartStrategies.noRestart());

        final String tablePath = storeLocation + CarbonCommonConstants.FILE_SEPARATOR + databaseName + CarbonCommonConstants.FILE_SEPARATOR + tableName;

        // Build source stream.
        final TestSource source = new TestSource("{\"stringField\": \"test\", \"intField\": 26, \"shortField\": 26, \"longField\": 1234567, \"doubleField\": 23.3333, \"boolField\": false, \"dateField\": \"2019-03-02\", \"timeField\": \"2019-02-12 03:03:34\", \"decimalField\" : 55.35, \"binaryField\" : \"abc\"}");
        final DataStreamSource<String> stream = environment.addSource(source);
        for (int i = 1; i < 5; i++) {
            stream.union(environment.addSource(new TestSource("{\"stringField\": \"test\", \"intField\": 26, \"shortField\": 26, \"longField\": 1234567, \"doubleField\": 23.3333, \"boolField\": false, \"dateField\": \"2019-03-02\", \"timeField\": \"2019-02-12 03:03:34\", \"decimalField\" : 55.35, \"binaryField\" : \"abc\"}")));
        }
        final StreamingFileSink<String> streamSink = StreamingFileSink.forBulkFormat(
            new Path(ProxyFileSystem.DEFAULT_URI),
            CarbonWriterFactoryBuilder.get("S3").build("default", tableName, tablePath, new Properties(), writerProperties, carbonProperties)
        ).build();
        stream.addSink(streamSink);

        // Execute the flink environment
        try {
            environment.execute();
            streamSink.close();
        } catch (Exception exception) {
            // TODO
            throw new UnsupportedOperationException(exception);
        }

        // Check result.
        checkAnswer(
            sql("select * from " + tableName + " order by stringfield limit 1"),
            new Object[]{"test1", 26, 26, 1234567, 23.3333, false, java.sql.Date.valueOf("2019-03-02"), Timestamp.valueOf("2019-02-12 03:03:34"), 55.35}
        );
        checkAnswer(
            sql("select count(1) from " + tableName),
            new Object[]{TestSource.DATA_COUNT.get()}
        );
    }

    private void setSparkSessionConfiguration(String accessKey, String secretKey, String endpoint) {
        final SQLConf sparkSessionConfiguration = Spark2TestQueryExecutor.spark().sessionState().conf();
        sparkSessionConfiguration.setConfString("fs.s3.access.key", accessKey);
        sparkSessionConfiguration.setConfString("fs.s3.secret.key", secretKey);
        sparkSessionConfiguration.setConfString("fs.s3.endpoint", endpoint);
        sparkSessionConfiguration.setConfString("fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        sparkSessionConfiguration.setConfString("fs.s3a.access.key", accessKey);
        sparkSessionConfiguration.setConfString("fs.s3a.secret.key", secretKey);
        sparkSessionConfiguration.setConfString("fs.s3a.endpoint", endpoint);
        sparkSessionConfiguration.setConfString("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
    }

    private void checkAnswer(final Dataset<Row> result, Object[] expectedResult) {
        result.show();
    }

}
