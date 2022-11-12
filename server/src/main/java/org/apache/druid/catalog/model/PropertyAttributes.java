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

package org.apache.druid.catalog.model;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;

import java.util.HashMap;
import java.util.Map;

public class PropertyAttributes
{
  public static final String IS_SQL_FN_PARAM_KEY = "sqlFnArg";
  public static final String IS_SQL_FN_OPTIONAL = "optional";
  public static final String IS_PARAMETER = "param";
  public static final String TYPE_NAME = "typeName";
  public static final String SQL_JAVA_TYPE = "sqlJavaType";

  public static final Map<String, Object> SQL_FN_PARAM =
      ImmutableMap.of(IS_SQL_FN_PARAM_KEY, true);
  public static final Map<String, Object> OPTIONAL_SQL_FN_PARAM =
      ImmutableMap.of(IS_SQL_FN_PARAM_KEY, true, IS_SQL_FN_OPTIONAL, true);
  public static final Map<String, Object> TABLE_PARAM =
      ImmutableMap.of(IS_PARAMETER, true);
  public static final Map<String, Object> SQL_AND_TABLE_PARAM =
      ImmutableMap.of(IS_SQL_FN_PARAM_KEY, true, IS_PARAMETER, true);

  public static boolean isSqlFunctionParameter(PropertyDefn<?> defn)
  {
    return defn.attributes().get(IS_SQL_FN_PARAM_KEY) == Boolean.TRUE;
  }

  public static boolean isOptional(PropertyDefn<?> defn)
  {
    return defn.attributes().get(IS_SQL_FN_OPTIONAL) == Boolean.TRUE;
  }

  public static String typeName(PropertyDefn<?> defn)
  {
    return (String) defn.attributes().get(TYPE_NAME);
  }

  public static Class<?> sqlParameterType(PropertyDefn<?> defn)
  {
    return (Class<?>) defn.attributes().get(SQL_JAVA_TYPE);
  }

  public static boolean isExternTableParameter(PropertyDefn<?> defn)
  {
    return defn.attributes().get(IS_PARAMETER) == Boolean.TRUE;
  }

  public static Map<String, Object> merge(Map<String, Object> attribs1, Map<String, Object> attribs2)
  {
    if (attribs1 == null) {
      return attribs2;
    }
    if (attribs2 == null) {
      return attribs1;
    }

    Map<String, Object> merged = new HashMap<>(attribs1);
    merged.putAll(attribs2);
    return ImmutableMap.copyOf(merged);
  }
}
