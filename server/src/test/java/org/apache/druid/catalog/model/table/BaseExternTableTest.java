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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.ISE;

import java.util.List;
import java.util.Map;

public class BaseExternTableTest
{
  protected final ObjectMapper mapper = DefaultObjectMapper.INSTANCE;

  protected PropertyDefn<?> findProperty(List<PropertyDefn<?>> props, String name)
  {
    for (PropertyDefn<?> prop : props) {
      if (prop.name().equals(name)) {
        return prop;
      }
    }
    return null;
  }

  protected Map<String, Object> toMap(InputFormat format)
  {
    try {
      return mapper.convertValue(format, ExternalTableDefn.MAP_TYPE_REF);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
  }

  protected String toJsonString(Map<String, Object> formatMap)
  {
    try {
      return mapper.writeValueAsString(formatMap);
    }
    catch (Exception e) {
      throw new ISE(e, "bad conversion");
    }
  }

  protected String formatToJson(InputFormat format)
  {
    Map<String, Object> formatMap = toMap(format);
    formatMap.remove("columns");
    return toJsonString(formatMap);
  }

  protected boolean hasParam(TableFunction fn, String key)
  {
    for (ParameterDefn param : fn.parameters()) {
      if (param.name().equals(key)) {
        return true;
      }
    }
    return false;
  }
}
