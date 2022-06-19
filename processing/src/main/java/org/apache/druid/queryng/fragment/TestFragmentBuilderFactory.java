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

package org.apache.druid.queryng.fragment;

import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.queryng.config.QueryNGConfig;

/**
 * Test version of the fragment factory which enables Query NG only if
 * the {@code druid.queryng.enable} system property is set, and then,
 * only for scan queries.
 */
public class TestFragmentBuilderFactory implements FragmentBuilderFactory
{
  private static final String ENABLED_KEY = QueryNGConfig.CONFIG_ROOT + ".enabled";
  private static final boolean ENABLED = Boolean.parseBoolean(System.getProperty(ENABLED_KEY));

  @Override
  public FragmentBuilder create(Query<?> query, ResponseContext responseContext)
  {
    //if (!ENABLED) {
    //  return null;
    //}
    if (!(query instanceof ScanQuery)) {
      return null;
    }
    return new FragmentBuilderImpl(query.getId(), 0, responseContext);
  }
}
