/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.testsEx.indexer;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.java.util.common.Pair;
import org.apache.druid.testsEx.categories.AzureDeepStorage;
import org.apache.druid.testsEx.config.Configure;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.apache.druid.testsEx.config.DruidTestRunnerFactory;
import org.apache.druid.testsEx.config.Initializer;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.List;

/**
 * IMPORTANT:
 * To run this test, you must:
 * 1) Set the bucket and path for your data. This can be done by setting -Ddruid.test.config.cloudBucket and
 *    -Ddruid.test.config.cloudPath or setting "cloud_bucket" and "cloud_path" in the config file.
 * 2) Copy wikipedia_index_data1.json, wikipedia_index_data2.json, and wikipedia_index_data3.json
 *    located in integration-tests/src/test/resources/data/batch_index/json to your Azure at the location set in step 1.
 * 3) Provide -Doverride.config.path=<PATH_TO_FILE> with Azure credentials/configs set. See
 *    integration-tests/docker/environment-configs/override-examples/azure for env vars to provide.
 */

@RunWith(DruidTestRunner.class)
@Category(AzureDeepStorage.class)
public class ITAzureToAzureParallelIndexTest extends AbstractAzureInputSourceParallelIndexTest
{

//  @Configure
//  public static void configure(Initializer.Builder builder)
//  {
//    builder.propertyEnvVarBinding("druid.my.bucket", "ULTIMATE_ANSWER");
//  }

  @Test
  public void testAzureIndexData() throws Exception
  {
    doTest(new Pair<>("objects",
                      ImmutableList.of(
                          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + "wikipedia_index_data1.json"),
                          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + "wikipedia_index_data2.json"),
                          ImmutableMap.of("bucket", "%%BUCKET%%", "path", "%%PATH%%" + "wikipedia_index_data3.json")
                      )
    ), new Pair<>(false, false));
  }
}
