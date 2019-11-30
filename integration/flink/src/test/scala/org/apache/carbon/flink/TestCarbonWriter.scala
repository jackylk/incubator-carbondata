package org.apache.carbon.flink

import java.util.Properties

import org.apache.carbondata.core.constants.CarbonCommonConstants
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.spark.sql.Row
import org.apache.spark.sql.test.util.QueryTest
import org.junit.Test

import org.apache.carbondata.common.exceptions.sql.MalformedCarbonCommandException
import org.apache.carbondata.core.datastore.impl.FileFactory
import org.apache.carbondata.core.exception.ConcurrentOperationException
import org.apache.carbondata.core.util.path.CarbonTablePath

class TestCarbonWriter extends QueryTest {

  val tableName = "test_flink"

  @Test
  def testLocal(): Unit = {
    sql(s"drop table if exists $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
      """.stripMargin
    ).collect()

    try {
      val rootPath = System.getProperty("user.dir") + "/target/test-classes"

      val dataTempPath = rootPath + "/data/temp/"
      val dataPath = rootPath + "/data/"

      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, dataPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 10000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): String = {
          Thread.sleep(1L)
          "{\"stringField\": \"test" + index + "\", \"intField\": " + index + ", \"shortField\": 12345}"
        }

        @throws[InterruptedException]
        override def onFinish(): Unit = {
          Thread.sleep(5000L)
        }
      }
      val stream = environment.addSource(source)
      val factory = CarbonWriterFactory.builder("Local").build(
        "default",
        tableName,
        tablePath,
        new Properties,
        writerProperties,
        carbonProperties
      )
      val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

      stream.addSink(streamSink)

      try environment.execute
      catch {
        case exception: Exception =>
          // TODO
          throw new UnsupportedOperationException(exception)
      }

      checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(0)))

      var exception = intercept[MalformedCarbonCommandException]{
        sql(s"alter table $tableName collect segments options ('batch' = 's')")
      }
      assertResult(true)(exception.getMessage.contains("'batch' option must be integer: s"))
      exception = intercept[MalformedCarbonCommandException]{
        sql(s"alter table $tableName collect segments options ('batch' = '-1')")
      }
      assertResult(true)(exception.getMessage.contains("'batch' option must be greater than 0: -1"))

      val loadDetailPath = CarbonTablePath.getLoadDetailsDir(tablePath)
      val segmentMetaPath = CarbonTablePath.getSegmentFilesLocation(tablePath)
      val numDetailFiles = FileFactory.getCarbonFile(loadDetailPath).listFiles().length
      val numSegmentFiles = FileFactory.getCarbonFile(segmentMetaPath).listFiles().count(_.getName.startsWith("_"))

      sql(s"alter table $tableName collect segments options ('batch' = '1')").collect()
      assertResult(numDetailFiles - 2)(FileFactory.getCarbonFile(loadDetailPath).listFiles().length)
      assertResult(numSegmentFiles - 1)(FileFactory.getCarbonFile(segmentMetaPath).listFiles().count(_.getName.startsWith("_")))
      assertResult(false)(FileFactory.isFileExist(loadDetailPath + "/snapshot"))

      sql(s"alter table $tableName collect segments options ('batch' = '1')").collect()
      assertResult(numDetailFiles - 4)(FileFactory.getCarbonFile(loadDetailPath).listFiles().length)
      assertResult(numSegmentFiles - 2)(FileFactory.getCarbonFile(segmentMetaPath).listFiles().count(_.getName.startsWith("_")))

      sql(s"alter table $tableName collect segments options ('batch' = '100')").collect()
      assertResult(0)(FileFactory.getCarbonFile(loadDetailPath).listFiles().length)
      assertResult(0)(FileFactory.getCarbonFile(segmentMetaPath).listFiles().count(_.getName.startsWith("_")))

      sql(s"show segments for table $tableName").show(false)
      checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(dataCount)))
    } finally {
      sql(s"drop table if exists $tableName").collect()
    }
  }

  @Test
  def testConcurrent(): Unit = {
    sql(s"drop table if exists $tableName").collect()
    sql(
      s"""
         | CREATE TABLE $tableName (stringField string, intField int, shortField short)
         | STORED AS carbondata
      """.stripMargin
    ).collect()

    try {
      val rootPath = System.getProperty("user.dir") + "/target/test-classes"

      val dataTempPath = rootPath + "/data/temp/"
      val dataPath = rootPath + "/data/"

      val tablePath = storeLocation + "/" + tableName + "/"

      val writerProperties = newWriterProperties(dataTempPath, dataPath, storeLocation)
      val carbonProperties = newCarbonProperties(storeLocation)

      val environment = StreamExecutionEnvironment.getExecutionEnvironment
      environment.setParallelism(1)
      environment.enableCheckpointing(2000L)
      environment.setRestartStrategy(RestartStrategies.noRestart)

      val dataCount = 10000
      val source = new TestSource(dataCount) {
        @throws[InterruptedException]
        override def get(index: Int): String = {
          Thread.sleep(1L)
          "{\"stringField\": \"test" + index + "\", \"intField\": " + index + ", \"shortField\": 12345}"
        }

        @throws[InterruptedException]
        override def onFinish(): Unit = {
          Thread.sleep(5000L)
        }
      }
      val stream = environment.addSource(source)
      val factory = CarbonWriterFactory.builder("Local").build(
        "default",
        tableName,
        tablePath,
        new Properties,
        writerProperties,
        carbonProperties
      )
      val streamSink = StreamingFileSink.forBulkFormat(new Path(ProxyFileSystem.DEFAULT_URI), factory).build

      stream.addSink(streamSink)

      try environment.execute
      catch {
        case exception: Exception =>
          // TODO
          throw new UnsupportedOperationException(exception)
      }

      checkAnswer(sql(s"select count(1) from $tableName"), Seq(Row(0)))

      val thread = new Thread(new Runnable {
        override def run(): Unit =
          sql(s"alter table $tableName collect segments options ('batch' = '1')")
      })
      thread.start()

      Thread.sleep(10L)
      val ex = intercept[ConcurrentOperationException] {
        sql(s"alter table $tableName collect segments options ('batch' = '1')")
      }
      assertResult(true)(ex.getMessage.contains("collect segment is in progress"))

      thread.join()
    } finally {
      sql(s"drop table if exists $tableName").collect()
    }
  }

  private def newWriterProperties(
                                   dataTempPath: String,
                                   dataPath: String,
                                   storeLocation: String) = {
    val properties = new Properties
    properties.setProperty(CarbonLocalProperty.DATA_TEMP_PATH, dataTempPath)
    properties.setProperty(CarbonLocalProperty.DATA_PATH, dataPath)
    properties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
    properties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024")
    properties
  }

  private def newCarbonProperties(storeLocation: String) = {
    val properties = new Properties
    properties.setProperty(CarbonCommonConstants.CARBON_TIMESTAMP_FORMAT,
      CarbonCommonConstants.CARBON_TIMESTAMP_DEFAULT_FORMAT)
    properties.setProperty(CarbonCommonConstants.CARBON_DATE_FORMAT,
      CarbonCommonConstants.CARBON_DATE_DEFAULT_FORMAT)
    properties.setProperty(CarbonCommonConstants.STORE_LOCATION, storeLocation)
    properties.setProperty(CarbonCommonConstants.UNSAFE_WORKING_MEMORY_IN_MB, "1024")
    properties
  }

}
