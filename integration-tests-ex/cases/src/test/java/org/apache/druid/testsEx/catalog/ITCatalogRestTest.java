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

package org.apache.druid.testsEx.catalog;

import com.google.inject.Inject;
import org.apache.druid.catalog.guice.Catalog;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

/**
 * Light sanity check of the Catalog REST API. Functional testing is
 * done via a unit test. Here we simply ensure that the Jersey plumbing
 * works as intended.
 */
@RunWith(DruidTestRunner.class)
@Category(Catalog.class)
public class ITCatalogRestTest
{
  @Inject
  private DruidClusterClient clusterClient;

  @Test
  public void testLifecycle()
  {
    CatalogClient client = new CatalogClient(clusterClient);
  }

}
