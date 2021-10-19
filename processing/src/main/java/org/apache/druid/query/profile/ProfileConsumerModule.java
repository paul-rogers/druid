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

import org.apache.druid.guice.JsonConfigProvider;

import com.google.inject.Binder;
import com.google.inject.Module;

public class ProfileConsumerModule implements Module
{
  static final String PROFILE_PROPERTY_PREFIX = "druid.profile";
  static final String PROFILE_CONSUMER_PROPERTY_PREFIX = PROFILE_PROPERTY_PREFIX + ".consumer";

  @Override
  public void configure(Binder binder)
  {
    binder.bind(ProfileConsumer.class).toProvider(ProfileConsumerProvider.class); //.in(ManageLifecycle.class);
    JsonConfigProvider.bindWithDefault(
        binder,
        PROFILE_CONSUMER_PROPERTY_PREFIX,
        ProfileConsumerProvider.class,
        NullProfileConsumerProvider.class
    );
  }
}
