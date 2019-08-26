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
package org.apache.carbon.flink

import java.util.Random
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.streaming.api.functions.source.SourceFunction

@SerialVersionUID(-6383469866617033513L)
final class TestSource(value: String) extends SourceFunction[String] {

  @throws[Exception]
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    for (i <- 1 to 50) {
      Thread.sleep(new Random().nextInt(1000))
      sourceContext.collectWithTimestamp(value.replace("test", "test" + i), System.currentTimeMillis)
      TestSource.DATA_COUNT.incrementAndGet()
    }
  }

  override def cancel(): Unit = {
    // to do nothing.
  }

}

object TestSource {

  val DATA_COUNT = new AtomicInteger(0)

}