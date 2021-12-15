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

package org.apache.druid.cli;

import com.google.inject.Binder;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.TypeLiteral;
import com.google.inject.multibindings.Multibinder;
import org.junit.Test;

import javax.inject.Inject;
import javax.inject.Qualifier;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Set;


public class GuiceMockup
{

  @Qualifier
  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ServiceMarker
  {
  }

  @Qualifier
  @Target({ElementType.FIELD, ElementType.PARAMETER, ElementType.METHOD})
  @Retention(RetentionPolicy.RUNTIME)
  public @interface ServiceConstant
  {
    String service();
  }

  public static void addResource(Binder binder, String service)
  {
    Multibinder.newSetBinder(binder, new TypeLiteral<String>() {}, ServiceMarker.class)
               .addBinding()
               .toInstance(service);
  }

  public static class TargetClass
  {
    final Set<String> services;

    @Inject
    public TargetClass(@ServiceMarker Set<String> services)
    {
      this.services = services;
    }
  }

  public static class TestModule implements Module
  {
    @Override
    public void configure(Binder binder)
    {
      addResource(binder, "foo/bar");
      addResource(binder, "foo/mumble");
    }
  }

  @Test
  public void testSomething()
  {
    final Injector injector = Guice.createInjector(new TestModule());
    TargetClass target = injector.getInstance(TargetClass.class);
    System.out.println(target.services);
  }
}
