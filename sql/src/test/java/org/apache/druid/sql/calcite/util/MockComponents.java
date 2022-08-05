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

package org.apache.druid.sql.calcite.util;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import org.apache.druid.guice.LazySingleton;
import org.apache.druid.server.security.Access;
import org.apache.druid.server.security.AllowAllAuthenticator;
import org.apache.druid.server.security.AuthConfig;
import org.apache.druid.server.security.AuthenticationResult;
import org.apache.druid.server.security.AuthenticatorMapper;
import org.apache.druid.server.security.Authorizer;
import org.apache.druid.server.security.AuthorizerMapper;
import org.apache.druid.server.security.ResourceType;

import java.util.Map;

public class MockComponents
{
  public static class MockAuthorizerMapper extends AuthorizerMapper
  {
    public MockAuthorizerMapper()
    {
      super(null);
    }

    @Override
    public Authorizer getAuthorizer(String name)
    {
      return (authenticationResult, resource, action) -> {
        if (authenticationResult.getIdentity().equals(CalciteTests.TEST_SUPERUSER_NAME)) {
          return Access.OK;
        }

        switch (resource.getType()) {
          case ResourceType.DATASOURCE:
            if (resource.getName().equals(CalciteTests.FORBIDDEN_DATASOURCE)) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.VIEW:
            if (resource.getName().equals("forbiddenView")) {
              return new Access(false);
            } else {
              return Access.OK;
            }
          case ResourceType.QUERY_CONTEXT:
            return Access.OK;
          default:
            return new Access(false);
        }
      };
    }
  }

  public static class MockAuthenticatorMapper extends AuthenticatorMapper
  {
    public MockAuthenticatorMapper()
    {
      super(ImmutableMap.of(
          AuthConfig.ALLOW_ALL_NAME,
          new AllowAllAuthenticator()
          {
            @Override
            public AuthenticationResult authenticateJDBCContext(Map<String, Object> context)
            {
              return new AuthenticationResult((String) context.get("user"), AuthConfig.ALLOW_ALL_NAME, null, null);
            }
          }
      ));
    }
  }

  public static class MockComponentsModule implements com.google.inject.Module
  {
    @Override
    public void configure(Binder binder)
    {
      binder.bind(AuthorizerMapper.class).to(MockAuthorizerMapper.class).in(LazySingleton.class);
      binder.bind(AuthenticatorMapper.class).to(MockAuthenticatorMapper.class).in(LazySingleton.class);
    }
  }
}
