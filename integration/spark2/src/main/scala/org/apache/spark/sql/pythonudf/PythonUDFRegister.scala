/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.pythonudf

import java.io.{ByteArrayOutputStream, File, FileWriter, InputStream}
import java.util.Arrays

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.api.python.{PythonBroadcast, PythonFunction, PythonUtils}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.python.UserDefinedPythonFunction
import org.apache.spark.sql.types.{DataType, StringType}

import org.apache.carbondata.core.datastore.impl.FileFactory

class PythonUDFRegister {

  def registerPythonUDF(spark: SparkSession,
      udfName: String,
      funcName: String,
      script: String,
      libraryIncludes: Array[String],
      returnType: DataType = StringType): Unit = {
    val fileName = generateScriptFile(funcName, script, returnType)

    val pathElements = new ArrayBuffer[String]
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    pathElements += Seq(sys.env("SPARK_HOME"), "python").mkString(File.separator)
    pathElements ++= libraryIncludes
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    val pythonExec = spark.sparkContext.getConf.get("spark.python.exec", "python")

    val pb = new ProcessBuilder(Arrays.asList(pythonExec, fileName))
    val workerEnv = pb.environment()
    workerEnv.put("PYTHONPATH", pythonPath)
    // This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
    workerEnv.put("PYTHONUNBUFFERED", "YES")
    val worker = pb.start()

    val stream = worker.getInputStream
    val errorStream = worker.getErrorStream
    worker.waitFor()
    val inBinary = getBinary(stream)
    val errBinary = getBinary(errorStream)
    if (errBinary.length  > 0) {
      throw new Exception(new String(errBinary))
    }
    worker.destroy()
    FileFactory.deleteFile(fileName, FileFactory.getFileType(fileName))
    // TODO handle big udf bigger than 1 MB, they supposed to be broadcasted.
    val function = PythonFunction(
      inBinary,
      new java.util.HashMap[String, String](),
      new java.util.ArrayList[String](),
      pythonExec, spark.sparkContext.getConf.get("spark.python.version", "2.7"),
      new java.util.ArrayList[Broadcast[PythonBroadcast]](),
      null)
    spark.udf.registerPython(
      udfName,
      UserDefinedPythonFunction(udfName, function, returnType, 0, true))
  }

  private def generateScriptFile(funcName: String, script: String, returnType: DataType): String = {
    // scalastyle:off
    val gen =
      s"""
         |import os
         |import sys
         |from pyspark.serializers import CloudPickleSerializer
         |from pyspark.sql.types import LongType,StringType,BinaryType,BooleanType,IntegerType,ByteType,ShortType,StructType,MapType,DecimalType,ArrayType
         |
         |${ script }
         |ser = CloudPickleSerializer()
         |pickled_command = bytearray(ser.dumps((${ funcName },
         |      ${ returnType.getClass.getSimpleName.replace("$", "") }())))
         |pickled_command
         |stdout_bin = os.fdopen(sys.stdout.fileno(), 'wb', 4)
         |stdout_bin.write(pickled_command)
     """.stripMargin
    // scalastyle:on
    val file = new File(System.getProperty("java.io.tmpdir") + "/python/" +
                        System.nanoTime() + ".py")
    file.getParentFile.mkdirs()
    val writer = new FileWriter(file)
    writer.write(gen)
    writer.close()
    file.getAbsolutePath
  }

  def getBinary(stream: InputStream): Array[Byte] = {
    val out = new ByteArrayOutputStream()
    while (stream.available() > 0) {
      out.write(stream.read())
    }
    stream.close()
    out.toByteArray
  }

}
