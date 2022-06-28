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

import com.google.api.client.util.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.external.ColumnSchema;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts SQL table function arguments for one subtype into a map of
 * JSON properties for that subtype.
 * <p>
 * For convenience, there are a few cases where there are two SQL argument
 * names that map to the same JSON property. For example, for CSV, both
 * "file" and "files" map to the same "files" JSON property. This class
 * ensures that only one of the overloads is present.
 */
public class SubTypeConverter
{
  final String sqlValue;
  final String jsonTypeValue;
  private final Map<String, PropertyConverter> properties;

  public SubTypeConverter(
      final String sqlValue,
      final String jsonTypeValue,
      final PropertyConverter[] properties)
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(sqlValue));
    this.sqlValue = sqlValue;
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jsonTypeValue));
    this.jsonTypeValue = jsonTypeValue;
    this.properties = new HashMap<>();
    for (PropertyConverter prop : properties) {
      if (this.properties.put(prop.sqlName, prop) != null) {
        throw new ISE(StringUtils.format(
            "Property %s is duplicated for type %s",
            prop.sqlName,
            sqlValue));
      }
    }
  }

  public Map<String, Object> convert(Map<String, ModelArg> args, List<ColumnSchema> columns)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    for (Map.Entry<String, PropertyConverter> entry : properties.entrySet()) {
      ModelArg arg = args.get(entry.getKey());
      if (arg == null) {
        continue;
      }
      PropertyConverter propConverter = entry.getValue();
      try {
        Object convertedValue = propConverter.sqlType.convert(arg.value());
        arg.consume();
        if (jsonMap.put(propConverter.jsonPath, convertedValue) != null) {
          List<String> overloads = getOverloads(propConverter.jsonPath);
          throw new IAE(StringUtils.format(
              "Specify only one of %s",
              overloads
              ));
        }
      }
      catch (IAE e) {
        throw new IAE(StringUtils.format(
            "The value [%s] is not valid for staging function argument %s of type %s: %s",
            arg.value(),
            propConverter.sqlName,
            propConverter.sqlType.sqlJavaType(),
            e.getMessage()));
      }
    }
    return jsonMap;
  }

  private List<String> getOverloads(String jsonPath)
  {
    List<String> overloads = new ArrayList<>();
    for (PropertyConverter prop : properties.values()) {
      if (prop.jsonPath.equals(jsonPath)) {
        overloads.add(prop.sqlName);
      }
    }
    return overloads;
  }

  public void gatherProperties(Map<String, PropertyConverter> propertyMap)
  {
    for (PropertyConverter prop : properties.values()) {
      PropertyConverter existing = propertyMap.put(prop.name(), prop);
      if (existing != null && existing.sqlJavaType() != prop.sqlJavaType()) {
        throw new ISE(StringUtils.format(
            "Property [%s] defined with conflicting types: [%s] and [%s]",
            prop.name(),
            prop.sqlJavaType().getSimpleName(),
            existing.sqlJavaType().getSimpleName()));
      }
    }
  }
}
