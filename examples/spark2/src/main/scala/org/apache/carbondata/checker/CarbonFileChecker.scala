package org.apache.carbondata.checker

import org.apache.carbondata.core.reader.CarbonIndexFileReader
import org.apache.carbondata.hadoop.CarbonInputSplit
import org.apache.carbondata.sdk.file.CarbonFileReaderBuilder
import org.apache.carbondata.sdk.file.CarbonFileReaderBuilder.FileFilter

import scala.collection.JavaConverters._
import scala.reflect.io.Path


class CarbonFileChecker() {

  class MyFileFilter(name: String, blockIds: Array[Int]) extends FileFilter {
    def this() {
      this("", Array())
    }

    override def filter(inputSplit: CarbonInputSplit): Boolean = {
      name.endsWith(inputSplit.getBlockPath) &&
        blockIds.contains(inputSplit.getBlockletId.toInt)
    }
  }

  def checkLong(filePath: String, colName: String,
                indexFile: String, dataFileName: String,
                blocketIds: Array[Int]): Unit = {
    // read index
    var leftLessThan = 300
    var leftGreaterThan = 300
    val indexReader = new CarbonIndexFileReader()
    val indexFilePath = filePath + "/" + indexFile
    val dataFilePath = filePath + "/" + dataFileName
    indexReader.openThriftReader(indexFilePath)
    val indexHead = indexReader.readIndexHeader()
    val idx = indexHead.table_columns.asScala.indexWhere(p => p.column_name.equalsIgnoreCase(colName))
    var indexMap = scala.collection.mutable.Map[Int, (Long, Long)]()
    var blockId = 0
    while (indexReader.hasNext) {
      val blockIndex = indexReader.readBlockIndexInfo()
      if (dataFilePath.contains(blockIndex.file_name)) {
        val max_min_index = blockIndex.block_index.min_max_index
        val maxVal = max_min_index.max_values.get(idx).getLong
        val minVal = max_min_index.min_values.get(idx).getLong
        println(blockIndex.file_name + s" $colName max $maxVal, min $minVal")
        indexMap += blockId -> (minVal, maxVal)
        blockId += 1
      }
    }

    // read file
    val colNames = Array(colName)
    val carbonReaderList = new CarbonFileReaderBuilder(filePath)
      .projection(colNames).filter(new MyFileFilter(dataFilePath, blocketIds)).build()

    carbonReaderList.asScala.zipWithIndex.foreach({
      case(reader, i) =>
        val blocketId = reader.getBlocketId.toInt
        var blocketMin = Long.MaxValue
        var blocketMax = Long.MinValue
        val indexMinMax = indexMap.get(blocketId)
        val indexMin = indexMinMax.get._1
        val indexMax = indexMinMax.get._2
        var count = 0
        var errorCount = 0
        while (reader.hasNext) {
          val value = reader.readNextRow().asInstanceOf[Array[Any]](0).asInstanceOf[Long]
          blocketMin = math.min(value, blocketMin)
          blocketMax = math.max(value, blocketMax)
          count += 1
          if (value > indexMax || value < indexMin) {

            errorCount += 1
            if (value > indexMax && leftGreaterThan > 0) {
              leftGreaterThan -= 1
              println(s"count id $count: Value $value greater than range [$indexMin, $indexMax]")
            } else if (value < indexMin && leftLessThan > 0) {
              leftLessThan -= 1
              println(s"count id $count: Value $value less than range [$indexMin, $indexMax]")
            }
          }
        }
        println(s"check $count values.")
        if (blocketMin == indexMin && blocketMax == indexMinMax.get._2) {
          println(s"Block $blocketId result is same.")
        } else {
          println(s"Block $blocketId, max $blocketMax, min $blocketMin. Index min $indexMin " +
            s" Index max $indexMax error percent: " + errorCount.toFloat/count)
        }
    })
  }
}
