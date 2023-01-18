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

package org.apache.druid.queryng.guice;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Injector;
import org.apache.druid.guice.StartupInjectorBuilder;
import org.apache.druid.query.Druids;
import org.apache.druid.query.Query;
import org.apache.druid.queryng.config.QueryNGConfig;
import org.apache.druid.queryng.fragment.QueryManagerFactory;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class QueryNGModuleTest
{
  @Test
  public void testEnabled()
  {
    Properties props = new Properties();
    props.setProperty(QueryNGConfig.CONFIG_ROOT + ".enabled", "true");
    Injector injector = new StartupInjectorBuilder()
        .add(new QueryNGModule())
        .withProperties(props)
        .build();
    assertNotNull(injector.getInstance(QueryManagerFactory.class));
    QueryNGConfig config = injector.getInstance(QueryNGConfig.class);
    assertTrue(config.enabled());
    assertFalse(config.requireContext());

    Query<?> query = Druids.newScanQueryBuilder()
        .eternityInterval()
        .dataSource("foo")
        .build();
    assertTrue(config.isEnabled(query));
  }

  @Test
  public void testDisabled()
  {
    Properties props = new Properties();
    props.setProperty(QueryNGConfig.CONFIG_ROOT + ".enabled", "false");
    Injector injector = new StartupInjectorBuilder()
        .add(new QueryNGModule())
        .withProperties(props)
        .build();
    assertNotNull(injector.getInstance(QueryManagerFactory.class));
    QueryNGConfig config = injector.getInstance(QueryNGConfig.class);
    assertFalse(config.enabled());

    Query<?> query = Druids.newScanQueryBuilder()
        .eternityInterval()
        .dataSource("foo")
        .build();
    assertFalse(config.isEnabled(query));
  }

  @Test
  public void testConditional()
  {
    Properties props = new Properties();
    props.setProperty(QueryNGConfig.CONFIG_ROOT + ".enabled", "true");
    props.setProperty(QueryNGConfig.CONFIG_ROOT + ".requireContext", "true");
    Injector injector = new StartupInjectorBuilder()
        .add(new QueryNGModule())
        .withProperties(props)
        .build();
    assertNotNull(injector.getInstance(QueryManagerFactory.class));
    QueryNGConfig config = injector.getInstance(QueryNGConfig.class);
    assertTrue(config.enabled());

    Query<?> query = Druids.newScanQueryBuilder()
        .eternityInterval()
        .dataSource("foo")
        .build();
    assertFalse(config.isEnabled(query));

    query = Druids.newScanQueryBuilder()
        .context(ImmutableMap.of(QueryNGConfig.CONTEXT_VAR, true))
        .eternityInterval()
        .dataSource("foo")
        .build();
    assertTrue(config.isEnabled(query));
  }
}
