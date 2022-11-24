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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.HttpInputSourceDefn;
import org.apache.druid.guice.annotations.Json;

public class HttpOperatorConversion extends CatalogExternalTableOperatorConversion
{
  public static final String FUNCTION_NAME = "http";

  @Inject
  public HttpOperatorConversion(
      final TableDefnRegistry registry,
      @Json final ObjectMapper jsonMapper
  )
  {
    super(FUNCTION_NAME, registry, HttpInputSourceDefn.TABLE_TYPE, jsonMapper);
  }
}
