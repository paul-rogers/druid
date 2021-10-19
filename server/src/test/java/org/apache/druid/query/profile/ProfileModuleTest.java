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

package org.apache.druid.query.profile;

import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.name.Names;
import org.apache.druid.guice.GuiceInjectors;
import org.apache.druid.guice.JsonConfigProvider;
import org.apache.druid.guice.JsonConfigurator;
import org.apache.druid.initialization.DruidModule;
import org.apache.druid.initialization.Initialization;
import org.apache.druid.query.profile.ProfileConsumer.ProfileConsumerStub;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class ProfileModuleTest
{
  private final JsonConfigProvider<ProfileConsumerProvider> provider =
      JsonConfigProvider.of(
          ProfileConsumerModule.PROFILE_CONSUMER_PROPERTY_PREFIX,
          ProfileConsumerProvider.class,
          NullProfileConsumerProvider.class);
  private final Injector injector = makeInjector();

  private Injector makeInjector()
  {
    return Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(new ProfileConsumerModule(),
            new DruidModule()
            {
              @Override
              public void configure(Binder binder)
              {
                binder.bind(Key.get(String.class, Names.named("serviceName"))).toInstance("some service");
                binder.bind(Key.get(Integer.class, Names.named("servicePort"))).toInstance(0);
                binder.bind(Key.get(Integer.class, Names.named("tlsServicePort"))).toInstance(-1);
              }

              @Override
              public List<? extends com.fasterxml.jackson.databind.Module> getJacksonModules()
              {
                return new ArrayList<>();
              }
            }
          )
    );
  }

  @Test
  public void testDefault()
  {
    final Properties properties = new Properties();
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final ProfileConsumer profileConsumer = (ProfileConsumer) provider.get().get().get();
    assertNotNull(profileConsumer);
    assertTrue(profileConsumer instanceof ProfileConsumerStub);
  }

  @Test
  public void testNull()
  {
    final Properties properties = new Properties();
    properties.put(
        ProfileConsumerModule.PROFILE_CONSUMER_PROPERTY_PREFIX + ".type",
        NullProfileConsumerProvider.TYPE_NAME);
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final ProfileConsumer profileConsumer = (ProfileConsumer) provider.get().get().get();
    assertNotNull(profileConsumer);
    assertTrue(profileConsumer instanceof ProfileConsumerStub);
  }

  @Test
  public void testSimpleDefaultPath()
  {
    final Properties properties = new Properties();
    properties.put(
        ProfileConsumerModule.PROFILE_CONSUMER_PROPERTY_PREFIX + ".type",
        SimpleFileProfileConsumerProvider.TYPE_NAME);
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final ProfileConsumer profileConsumer = (ProfileConsumer) provider.get().get().get();
    assertNotNull(profileConsumer);
    assertTrue(profileConsumer instanceof ProfileWriter);
    ProfileWriter simpleConsumer = (ProfileWriter) profileConsumer;
    assertEquals(SimpleFileProfileConsumerProvider.DEFAULT_DIR, simpleConsumer.getDir().getPath());
  }

  @Test
  public void testSimpleWithPath()
  {
    final Properties properties = new Properties();
    properties.put(
        ProfileConsumerModule.PROFILE_CONSUMER_PROPERTY_PREFIX + ".type",
        SimpleFileProfileConsumerProvider.TYPE_NAME);
    properties.put(
        ProfileConsumerModule.PROFILE_CONSUMER_PROPERTY_PREFIX + ".dir",
        "/tmp/foo/profiles");
    provider.inject(properties, injector.getInstance(JsonConfigurator.class));
    final ProfileConsumer profileConsumer = (ProfileConsumer) provider.get().get().get();
    assertNotNull(profileConsumer);
    assertTrue(profileConsumer instanceof ProfileWriter);
    ProfileWriter simpleConsumer = (ProfileWriter) profileConsumer;
    assertEquals("/tmp/foo/profiles", simpleConsumer.getDir().getPath());
  }

}
