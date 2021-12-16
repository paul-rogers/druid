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

package org.apache.druid.guice;

import com.google.inject.Binder;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

public class Services
{
  public static final String SERVICE_NAME_KEY = "serviceName";
  public static final String SERVICE_PORT_KEY = "servicePort";
  public static final String SERVICE_TLS_PORT_KEY = "tlsServicePort";
  public static final Named SERVICE_NAME = Names.named(SERVICE_NAME_KEY);
  public static final Named SERVICE_PORT = Names.named(SERVICE_PORT_KEY);
  public static final Named SERVICE_TLS_PORT = Names.named(SERVICE_TLS_PORT_KEY);
  public static final int NULL_TLS_PORT = -1;

  /**
   * Define an internal service with no external ports.
   */
  public static void bindService(Binder binder, String serviceName)
  {
    Services.bindService(binder, serviceName, 0, NULL_TLS_PORT);
  }

  /**
   * Define a service which exposes ports.
   */
  public static void bindService(Binder binder, String serviceName, int servicePort, int tlsServicePort)
  {
    binder.bindConstant().annotatedWith(SERVICE_NAME).to(serviceName);
    binder.bindConstant().annotatedWith(SERVICE_PORT).to(servicePort);
    binder.bindConstant().annotatedWith(SERVICE_TLS_PORT).to(tlsServicePort);
  }
}
