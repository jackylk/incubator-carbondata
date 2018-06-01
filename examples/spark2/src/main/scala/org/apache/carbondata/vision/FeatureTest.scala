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

package org.apache.carbondata.vision

import org.apache.commons.codec.binary.Hex

object FeatureTest {
  def main(args: Array[String]): Unit = {
    val vector =
      "e23b00a400240000730147d40a0907060e04020c0b0d0f050072009a66ed5bdd1dd58ab51ee957915a0b2cf" +
      "1fc740a63e7ce3c94c7902bcd0dbad5888660ace512f0a56997dd93b007790127b92a7892e07d233d3d45b4" +
      "814f3b60ddf0deae3b610fdfbebbbcb22dfa06ba9f07ba2831f06e7c9639f5ea3d53446939b5a64f572367a" +
      "e9204ccd1403caa6c64d77768aac7249685de6b2fd5a17446a7833b5e3698b7d910c60c9c7581781a9b0a54" +
      "320b1d715cedc98fbb2bb3009fc938bc4e2048f67b72e508686e6920e35c6928bedc7a797a2de7a0ff5fb6b" +
      "8f9b2b0d2ee44dc7fd262f1502d548556bc0c74190b7fe8533c3eb822820e81164e0ba545e5d27edf7e5b14" +
      "5de5d1671d081d0000d6006e00294e003bcb00a900d80001030008";
    val bytes = Hex.decodeHex(vector.toCharArray)
    // println(bytes.map(_ & 0xFF).mkString(","))
  }
}
