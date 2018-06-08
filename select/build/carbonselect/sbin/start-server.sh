#!/usr/bin/env bash

#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if [ -z "${CARBON_HOME}" ]; then
  export CARBON_HOME="$(cd "`dirname "$0"`/.."; pwd)"
fi

nohup java -cp "${CARBON_HOME}/jars/*" -Djava.library.path=${CARBON_HOME}/lib/vision org.apache.carbondata.service.server.CarbonServerExample ${CARBON_HOME}/conf/server/log4j.properties ${CARBON_HOME}/conf/server/carbonselect.properties > nohup.out 2>&1 &

echo $(hostname) started