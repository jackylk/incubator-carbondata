package org.apache.carbondata.examples

import org.apache.carbondata.core.metadata.datatype.DataTypes
import org.apache.carbondata.core.reader.CarbonIndexFileReader
import org.apache.carbondata.format.BlockIndex
import org.apache.carbondata.sdk.file.{CarbonReader, Field}

import scala.collection.mutable.ListBuffer
import scala.util.control.Breaks.{break, breakable}


object CarbonReaderExample {

  def main(args: Array[String]): Unit = {
    import org.apache.carbondata.checker.CarbonFileChecker
    val checker = new CarbonFileChecker
    checker.checkLong("/bdata00/zhangyunfan/carbondata/testdata/d_live_item_stats_1_4",
      "id", "100100042100001_batchno0-0-1532489931883.carbonindex",
      "part-2-100100042100001_batchno0-0-1532489931883.carbondata",
      (0 to 100).toArray)

    checker.checkLong("/bdata00/zhangyunfan/carbondata/testdata/m_device_distinct_1_4_0",
    "device_id", "134100000100001_batchno0-0-1532915792395.carbonindex",
      "part-0-134100000100001_batchno0-0-1532915792395.carbondata",
      (0 to 100).toArray)

    checker.checkLong("/bdata00/zhangyunfan/carbondata/testdata/test_bigint_1_4",
    "id", "0_batchno0-0-1532945815582.carbonindex",
    "part-0-0_batchno0-0-1532945815582.carbondata", (0 to 100).toArray)

    checker.checkLong("/bdata00/zhangyunfan/carbondata/testdata/partition_test_bigint_1_4",
    "id", "100100000100001_batchno0-0-1533125882436.carbonindex",
    "part-0-100100000100001_batchno0-0-1533125882436.carbondata", (0 to 100).toArray)

  }


  def createIndexReader(): Unit = {
    val fileName = "hdfs://haruna/data/carbondata/store/test_carbondata/d_live_item_stats/Fact/Part0/Segment_0/402_batchno0-0-1531207650037.carbonindex"
    val indexReader = new CarbonIndexFileReader()
    indexReader.openThriftReader(fileName)
    val indexHead = indexReader.readIndexHeader()
    val arrayBuff = ListBuffer[BlockIndex]()
    while (indexReader.hasNext) {
      arrayBuff += indexReader.readBlockIndexInfo()
    }
    arrayBuff
    for (e <- arrayBuff) println(e.asInstanceOf[BlockIndex].file_name)
    indexHead.table_columns
  }

  def createReader(): Unit = {
    val idField = new Field("id", DataTypes.LONG)
    val vvField = new Field("vv", DataTypes.LONG)
    val dateField = new Field("date", DataTypes.DATE)
    val allFields = Array(idField, vvField, dateField)
    val hdfsPath = "hdfs://haruna/data/carbondata/store/test_carbondata/bigint_test"
    ///bdata00/zhangyunfan/carbondata/testdata/d_live_item_stats_1_4
//    val filePath = "/bdata00/zhangyunfan/carbondata/testdata/bigint_test"
    val filePath = "/bdata00/zhangyunfan/carbondata/testdata/d_live_item_stats_1_4"
    val carbonReader1 = CarbonReader.builder(filePath, "d_live_item_stats_1_4").projection(Array("id")).build()
    var r = Array[Any]()
    breakable {
      while (carbonReader1.hasNext) {
        val row = (carbonReader1.readNextRow.asInstanceOf[Array[Any]])
        if (row(1) != 17718) {
          r = row
          break
        }
      }
    }

  }
}
