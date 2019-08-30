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

package org.apache.carbondata.core.constants;

import org.apache.carbondata.core.constants.CarbonVersionConstants;
import org.apache.commons.lang3.StringUtils;
import org.junit.Ignore;
import org.junit.Test;

import static junit.framework.TestCase.assertTrue;

public class CarbondataVersionUnitTest {

  @Test public void testCarbonVersionNotNull() {
    assertTrue(StringUtils.isNoneEmpty(CarbonVersionConstants.CARBONDATA_VERSION));
    assertTrue(StringUtils.isNoneEmpty(CarbonVersionConstants.CARBONDATA_BRANCH));
    assertTrue(StringUtils.isNoneEmpty(CarbonVersionConstants.CARBONDATA_REVISION));
    assertTrue(StringUtils.isNoneEmpty(CarbonVersionConstants.CARBONDATA_BUILD_DATE));
  }

  @Ignore
  @Test public void testCarbonBVersion() {
    assertTrue(CarbonVersionConstants.CARBONDATA_VERSION.contains(".B0"));
  }
}
