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

package org.apache.spark.sql.leo

import org.apache.carbondata.SparkS3Constants
import org.apache.carbondata.core.util.CarbonUtil
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.leo.image.Utils.{fromBytes, toBytes}
import org.apache.spark.sql.leo.image.{BasicTransformations, DataAugmentor, GeometricTransformations}
import org.apache.spark.sql.leo.util.obs.OBSUtil
import org.apache.spark.sql.pythonudf.PythonUDFRegister
import org.apache.spark.sql.types.{BooleanType, IntegerType, StructField, StructType}
import org.opencv.core.Core


object VisionSparkUDFs {

  /**
   * Utility method to register all Vision-related UDFs at once
   *
   * For more fine grained control over the parameters, consider registering UDFs manually
   * instead of calling this method
   *
   * @param sparkSession
   */
  def registerAll(sparkSession: SparkSession): Unit = {
    nu.pattern.OpenCV.loadShared()
    System.loadLibrary(Core.NATIVE_LIBRARY_NAME)

    // Images Augmentation UDFs
    registerCrop(sparkSession)
    registerResize(sparkSession)
    registerRotate(sparkSession)
    registerRotateWithBgColor(sparkSession)
    registerAddNoise(sparkSession)

    // Video UDFs
    val scriptsDirPath1 = System.getProperty("user.dir") + "/fleet/vision/src/main/scala/org/apache/spark/sql/leo/video/"
    VisionSparkUDFs.registerExtractFramesFromVideo(sparkSession, scriptsDirPath1)

    // Medical Images UDFs
    val scriptsDirPath2 = System.getProperty("user.dir") + "/fleet/vision/src/main/scala/org/apache/spark/sql/leo/medical/"
    VisionSparkUDFs.registerMedicalImageCrop(sparkSession, scriptsDirPath2)
  }


  /**
   * Image crop
   *
   * @param sparkSession
   */
  def registerCrop(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("crop",
      (bytes: Array[Byte], x: Int, y: Int, w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val croppedImg = BasicTransformations.crop(img, x, y, w, h)
        toBytes(croppedImg)
      })
  }


  /**
   * Image resize
   *
   * @param sparkSession
   */
  def registerResize(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("resize",
      (bytes: Array[Byte], w: Int, h: Int) => {
        val img = fromBytes(bytes)
        val resizedImg = BasicTransformations.resize(img, w, h)
        toBytes(resizedImg)
      })
  }


  /**
   * Image rotate (with white background color)
   *
   * @param sparkSession
   */
  def registerRotate(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("rotate",
      (bytes: Array[Byte], angle: Int) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle)
        toBytes(rotatedImg)
      })
  }


  /**
   * Image rotate with custom background color
   *
   * @param sparkSession
   */
  def registerRotateWithBgColor(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("rotateWithBgColor",
      (bytes: Array[Byte], angle: Int, bgColor: String) => {
        val img = fromBytes(bytes)
        val rotatedImg = GeometricTransformations.rotate(img, angle, bgColor)
        toBytes(rotatedImg)
      })
  }


  /**
   * Add random noise to image
   *
   * @param sparkSession
   */
  def registerAddNoise(sparkSession: SparkSession): Unit = {
    sparkSession.udf.register("addRandomNoise",
      (bytes: Array[Byte]) => {
        val img = fromBytes(bytes)
        val noisyImg = DataAugmentor.addRandomNoise(img, 1, 0.5)
        toBytes(noisyImg)
      })
  }

  def registerExtractFramesFromVideo(sparkSession: SparkSession, scriptsDirPath: String): Unit = {
    val localScriptsDirPath = if (scriptsDirPath.startsWith("obs")) {
      val tempDir = "/tmp/" + CarbonUtil.generateUUID()
      OBSUtil.copyToLocal(scriptsDirPath, tempDir, sparkSession, true)
      tempDir
    } else {
      scriptsDirPath
    }

    val script =
      s"""
         |import obs
         |import os
         |import sys
         |import time
         |
         |def generate_x_frames_per_sec(video_file, x, images_dir):
         |    import cv2
         |    sys.path.insert(0, '${ localScriptsDirPath }')
         |    from video_frames import Video
         |
         |    scratch_dir = '/tmp/naman/'
         |
         |    AK = '${ sparkSession.conf.get(SparkS3Constants.AK) }'
         |    SK = '${ sparkSession.conf.get(SparkS3Constants.SK) }'
         |    SERVER = '${ sparkSession.conf.get(SparkS3Constants.END_POINT) }'
         |    obsClient = obs.ObsClient(access_key_id=AK, secret_access_key=SK, server=SERVER)
         |
         |    start_time = time.time()
         |
         |    # Download file from OBS to local
         |    if not video_file.startswith('obs://'):
         |        raise ValueError('Video is not OBS path. Please upload video to OBS and rerun.')
         |    video_file = video_file[6:]
         |    bucket_name = video_file[:video_file.find('/')]
         |    video_file = video_file[len(bucket_name)+1:]
         |    local_video_dir = os.path.join(scratch_dir, 'videos')
         |    local_video_file = os.path.join(local_video_dir, os.path.basename(video_file))
         |    if not os.path.isdir(local_video_dir):
         |        os.mkdir(local_video_dir)
         |    scratch_dir = os.path.join(scratch_dir, os.path.basename(video_file))
         |    if not os.path.isdir(scratch_dir):
         |         os.mkdir(scratch_dir)
         |    obsClient.getObject(bucket_name, video_file, downloadPath=local_video_file)
         |
         |    # Process file
         |    try:
         |        count = 0
         |        with Video(local_video_file) as video:
         |            for i, img in video.get_x_frames_per_sec(x):
         |                cv2.imwrite(scratch_dir + '/{}.jpg'.format(i), img)
         |                count += 1
         |        success = True
         |        elapsed_time = int(time.time() - start_time)
         |
         |    except Exception as ex:
         |        print(ex)
         |        return False, 0, int(time.time() - start_time)
         |
         |    # Upload data to OBS (from local scratch_dir to OBS image_dir)
         |    if not images_dir.startswith('obs://'):
         |        raise ValueError('Output directory is not OBS path.')
         |    images_dir = images_dir[6:]
         |    bucket_name = images_dir[:images_dir.find('/')]
         |    images_dir = images_dir[len(bucket_name)+1:]
         |    images_dir = os.path.join(images_dir, os.path.basename(video_file))
         |    obsClient.putFile(bucket_name, images_dir, scratch_dir)
         |
         |    return success, count, elapsed_time
       """.stripMargin

    PythonUDFRegister.registerPythonUDF(
      sparkSession,
      "generate_x_frames_per_sec",
      "generate_x_frames_per_sec",
      script,
      Array[String](),
      new StructType(Array(
        StructField(name = "success", dataType = BooleanType, nullable = false),
        StructField(name = "num-files", dataType = IntegerType, nullable = false),
        StructField(name = "time", dataType = IntegerType, nullable = false)
      )),
      true
    )
  }

  def registerMedicalImageCrop(sparkSession: SparkSession, scriptsDirPath: String): Unit = {
    val localScriptsDirPath = if (scriptsDirPath.startsWith("obs")) {
      val tempDir = "/tmp/" + CarbonUtil.generateUUID()
      OBSUtil.copyToLocal(scriptsDirPath, tempDir, sparkSession, true)
      tempDir
    } else {
      scriptsDirPath
    }

    val script =
      s"""
         |import sys
         |import time
         |import obs
         |
         |def crop_file(file_path, images_dir):
         |    sys.path.insert(0, '${ localScriptsDirPath }')
         |    import wsi_crop
         |
         |    scratch_dir = '/tmp/naman/'
         |
         |    AK = '${ sparkSession.conf.get(SparkS3Constants.AK) }'
         |    SK = '${ sparkSession.conf.get(SparkS3Constants.SK) }'
         |    SERVER = '${ sparkSession.conf.get(SparkS3Constants.END_POINT) }'
         |    obsClient = obs.ObsClient(access_key_id=AK, secret_access_key=SK, server=SERVER)
         |
         |    start_time = time.time()
         |
         |    # Download file from OBS to local
         |    if not file_path.startswith('obs://'):
         |        raise ValueError('File is not OBS path. Please upload file to OBS and rerun.')
         |    file_path = file_path[6:]
         |    bucket_name = file_path[:file_path.find('/')]
         |    file_path = file_path[len(bucket_name)+1:]
         |    local_file_dir = os.path.join(scratch_dir, 'inputs')
         |    local_file_path = os.path.join(local_file_dir, os.path.basename(file_path))
         |    if not os.path.isdir(local_file_dir):
         |        os.mkdir(local_file_dir)
         |    scratch_dir = os.path.join(scratch_dir, os.path.basename(file_path))
         |    if not os.path.isdir(scratch_dir):
         |         os.mkdir(scratch_dir)
         |    obsClient.getObject(bucket_name, file_path, downloadPath=local_file_path)
         |
         |    start_time = time.time()
         |
         |    try:
         |        crop_processor = wsi_crop.CropProcess(10)
         |        crop_processor.crop(local_file_path, scratch_dir)
         |        elapsed_time = int(time.time() - start_time)
         |    except Exception as e:
         |        print(e)
         |        return False, int(time.time() - start_time)
         |
         |    # Upload data to OBS (from local scratch_dir to OBS image_dir)
         |    if not images_dir.startswith('obs://'):
         |        raise ValueError('Output directory is not OBS path.')
         |    images_dir = images_dir[6:]
         |    bucket_name = images_dir[:images_dir.find('/')]
         |    images_dir = images_dir[len(bucket_name)+1:]
         |    images_dir = os.path.join(images_dir, os.path.basename(file_path))
         |    obsClient.putFile(bucket_name, images_dir, scratch_dir)
         |
         |    return True, elapsed_time
       """.stripMargin

    PythonUDFRegister.registerPythonUDF(
      sparkSession,
      "crop_file",
      "crop_file",
      script,
      Array[String](),
      new StructType(Array(
        StructField(name = "success", dataType = BooleanType, nullable = false),
        StructField(name = "time", dataType = IntegerType, nullable = false)
      )),
      true
    )
  }

}
