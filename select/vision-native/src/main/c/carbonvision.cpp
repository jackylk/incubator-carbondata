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
#include <stdio.h>
#include <dlfcn.h>
#include <math.h>
#include <string.h>
#include "head/carbonvision.h"
#include "head/algorithm_plugin_common.h"
#include "head/user_formula_cpu.h"

using namespace std;

int gVecLen = 288;
handle_t  gHandle;
Plu_Host_CalcDist_Methods_S gMethods;

const char *paraInfo = "15016;;";

void *MallocMem(int size)
{
  if (size <= 0)
    return NULL;

  return malloc(size);
}

void FreeMem(void *addr)
{
  free(addr);
}

int MyPrintfLog(Log_level_E logLevel, const char *fmt, ...)
{
  int ret = 0;
  va_list args;

  va_start(args, fmt);
  ret = vprintf(fmt, args);
  va_end(args);
  printf("\n");

  return ret;
}

int InitFunc(const char *modelPath) {
  if (nullptr == gHandle) {
    Plu_Param_Set_S params = {0, NULL};
    Srs_Host_MemoryMethods_S memFunc;
    memFunc.pfnHostMalloc = MallocMem;
    memFunc.pfnHostFree = FreeMem;

    int initPlugin = Plu_Host_InitPlugin(modelPath, params, memFunc, MyPrintfLog, &gHandle);
    if (initPlugin != 0) {
      printf("Plu_Host_InitPlugin error!\n");
      return initPlugin;
    }

    int registMethod = Plu_Host_RegistSearchMethod(gHandle, &gMethods);
    if (registMethod != 0) {
      printf("Plu_Host_RegistSearchMethod error!\n");
      return registMethod;
    }
  }

  return 0;
}

int InitVector(JNIEnv *env, Plu_VecSet_S &vector, jbyteArray search, jint searchCount) {
  jbyte *value = env->GetByteArrayElements(search, 0);
  vector.address = (void *) value;
  vector.number = searchCount;
  vector.vecSize.dimNum = gVecLen;
  vector.vecSize.vectorSize = gVecLen;
  return 0;
}

int InitVectorNative(JNIEnv *env, Plu_VecSet_S &vector, jlong search, jint searchCount) {
  vector.address = (void *) search;
  vector.number = searchCount;
  vector.vecSize.dimNum = gVecLen;
  vector.vecSize.vectorSize = gVecLen;
  return 0;
}

void GetCalcDistancePara(Plu_Distance_Set_S &vecDistance, const int baseNumber, const int queryNumber) {
  vecDistance.number = queryNumber;
  vecDistance.distances = new Plu_Distance_S[queryNumber];
  for (int i = 0; i < queryNumber; i++) {
    vecDistance.distances[i].number = baseNumber;
    vecDistance.distances[i].distance = new float[baseNumber];
    memset(vecDistance.distances[i].distance, 0x0, baseNumber);
  }
}

JNIEXPORT jint JNICALL Java_org_apache_carbondata_vision_model_ModelManager_loadKNNSearchModel
(JNIEnv *env, jclass jc, jstring jModelPath) {
  return InitFunc(env->GetStringUTFChars(jModelPath, 0));
}

JNIEXPORT jint JNICALL Java_org_apache_carbondata_vision_algorithm_impl_KNNSearch_calcDistanceSet
(JNIEnv *env, jobject jobj, jbyteArray search, jbyteArray featureSet, jfloatArray output, jstring jModelPath) {
  int outputLength = env->GetArrayLength(output);
  int status = InitFunc(env->GetStringUTFChars(jModelPath, 0));
  if (status != 0) {
    return status;
  }

  Plu_VecSet_S gBaseVec;
  Plu_VecSet_S gQueryVec;
  InitVector(env, gQueryVec, search, 1);
  InitVector(env, gBaseVec, featureSet, outputLength);

  Plu_Distance_Set_S distance = {0, NULL};
  GetCalcDistancePara(distance, outputLength, 1);

  int result = gMethods.pfnCalcDistanceSetEx(gHandle, gBaseVec, gQueryVec, distance, paraInfo);

  env->ReleaseByteArrayElements(search, (jbyte *)gQueryVec.address, 0);
  env->ReleaseByteArrayElements(featureSet, (jbyte *) gBaseVec.address, 0);
  env->SetFloatArrayRegion(output, 0, outputLength, distance.distances[0].distance);

  return result;
}

JNIEXPORT jint JNICALL Java_org_apache_carbondata_vision_algorithm_impl_KNNSearch_calcDistanceSetNative
(JNIEnv *env, jobject jobj, jlong search, jlong featureSet, jfloatArray output, jstring jModelPath) {
  int outputLength = env->GetArrayLength(output);
  int status = InitFunc(env->GetStringUTFChars(jModelPath, 0));
  if (status != 0) {
    return status;
  }

  Plu_VecSet_S gBaseVec;
  Plu_VecSet_S gQueryVec;
  InitVectorNative(env, gQueryVec, search, 1);
  InitVectorNative(env, gBaseVec, featureSet, outputLength);

  Plu_Distance_Set_S distance = {0, NULL};
  GetCalcDistancePara(distance, outputLength, 1);

  int result = gMethods.pfnCalcDistanceSetEx(gHandle, gBaseVec, gQueryVec, distance, paraInfo);

  env->SetFloatArrayRegion(output, 0, outputLength, distance.distances[0].distance);

  return result;
}


void GetCalcDistanceVecPara(Plu_VecSet_S &vecSet, const int size) {
  if (size <= 0) {
    return;
  }
  int32_t *featureArray = new int32_t[288]{226,59,0,164,0,36,0,0,115,1,71,212,10,9,7,6,14,4,2,12,11,13,15,5,0,114,0,154,102,237,91,221,29,213,138,181,30,233,87,145,90,11,44,241,252,116,10,99,231,206,60,148,199,144,43,205,13,186,213,136,134,96,172,229,18,240,165,105,151,221,147,176,7,121,1,39,185,42,120,146,224,125,35,61,61,69,180,129,79,59,96,221,240,222,174,59,97,15,223,190,187,188,178,45,250,6,186,159,7,186,40,49,240,110,124,150,57,245,234,61,83,68,105,57,181,166,79,87,35,103,174,146,4,204,209,64,60,170,108,100,215,119,104,170,199,36,150,133,222,107,47,213,161,116,70,167,131,59,94,54,152,183,217,16,198,12,156,117,129,120,26,155,10,84,50,11,29,113,92,237,201,143,187,43,179,0,159,201,56,188,78,32,72,246,123,114,229,8,104,110,105,32,227,92,105,40,190,220,122,121,122,45,231,160,255,95,182,184,249,178,176,210,238,68,220,127,210,98,241,80,45,84,133,86,188,12,116,25,11,127,232,83,60,62,184,34,130,14,129,22,78,11,165,69,229,210,126,223,126,91,20,93,229,209,103,29,8,29,0,0,214,0,110,0,41,78,0,59,203,0,169,0,216,0,1,3,0,8};
  char *charArray = new char[288];

  for (int i =0; i < 288; i++) {
    charArray[i] = static_cast<char> (featureArray[i]);
  }

  vecSet.address = (void*) charArray;
  vecSet.number = size;
  vecSet.vecSize.dimNum = gVecLen;
  vecSet.vecSize.vectorSize = gVecLen;
}

int main(int argc, char **argv) {

  int status = InitFunc("/home/david/Documents/code/carbonstore/ai/vision-native/lib/intellifData");
  if (status != 0) {
    printf("failed to init handler");
    return status;
  }

  Plu_VecSet_S gBaseVec;
  Plu_VecSet_S gQueryVec;
  GetCalcDistanceVecPara(gBaseVec, 1);
  GetCalcDistanceVecPara(gQueryVec, 1);

  Plu_Distance_Set_S distance = {0, NULL};
  GetCalcDistancePara(distance, 1, 1);

  int result = gMethods.pfnCalcDistanceSetEx(gHandle, gBaseVec, gQueryVec, distance, paraInfo);
  printf("distance %f", distance.distances[0].distance[0]);

  return result;
}