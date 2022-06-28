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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import net.thisptr.jackson.jq.internal.misc.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.sql.calcite.external.ColumnSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Converts a list of table function arguments, and a SQL schema, into a
 * Druid input source and input format. Defined generically so we can use
 * it in other contexts if needed. The argument list is flat, with SQL-specific
 * names. The arguments match a set of objects, where each object can have
 * subtypes, indicated by some argument. The flat list is rewritten into a
 * tree of maps, in a form that can be used to convert to the target Java
 * object.
 */
public class ModelConverter<T>
{
  private final ObjectMapper objectMapper;
  private final Class<T> targetType;
  private final Map<String, ObjectConverter> components = new HashMap<>();
  private final List<PropertyConverter> properties;

  public ModelConverter(
      ObjectMapper objectMapper,
      Class<T> targetType,
      ObjectConverter[] components
  )
  {
    this.objectMapper = objectMapper;
    this.targetType = targetType;
    Map<String, PropertyConverter> propertyMap = new HashMap<>();
    for (ObjectConverter component : components) {
      if (this.components.put(component.jsonKey, component) != null) {
        throw new ISE(StringUtils.format(
            "Component %s is duplicated",
            component.jsonKey));
      }
      component.gatherProperties(propertyMap);
    }

    // Create an ordered list, to match the Calcite operands.
    // Sort by name so the order is deterministic across runs for
    // easier debugging.
    this.properties = Lists.newArrayList(propertyMap.values());
    this.properties.sort((p1, p2) -> p1.name().compareTo(p2.name()));
  }

  /**
   * Return the properties for this converter as an alphabetical list.
   * This is the set of function arguments (after more conversion) known
   * to Calcite table functions.
   */
  public List<PropertyConverter> properties()
  {
    return properties;
  }

  /**
   * Convert from an argument list and schema directly to the target Java
   * object.
   *
   * @param args list of arguments provided to the table function
   * @param columns list of columns in the "extend" clause following the
   * table function
   * @return the converted Java object
   */
  public T convert(Map<String, Object> args, List<ColumnSchema> columns)
  {
    return convertFromMap(convertToMap(args, columns), columns);
  }

  /**
   * Convert from an argument list and schema to a tree of maps that
   * provides the (possibly partial) set of values used to create the
   * target Java object. The values provided here can override another
   * set provided by the catalog to define the final set used to create
   * the Java object.
   *
   * @param args list of arguments provided to the table function
   * @param columns list of columns in the "extend" clause following the
   * table function
   * @return the converted property map tree
   */
  public Map<String, Object> convertToMap(Map<String, Object> args, List<ColumnSchema> columns)
  {
    Map<String, ModelArg> argMap = ModelArg.convertArgs(args);
    return argsToMap(argMap, columns);
  }

  /**
   * Convert property-style arguments to a JSON map, ready for Jackson
   * conversion to the implementation objects.
   */
  private Map<String, Object> argsToMap(Map<String, ModelArg> argMap, List<ColumnSchema> columns)
  {
    Map<String, Object> jsonMap = convertArgs(argMap, columns);
    ModelArg.verifyArgs(argMap);
    return jsonMap;
  }

  /**
   * Create the target Java object from a tree of maps that hold the properties
   * to be used to Jackson-serialize that object.
   *
   * @param jsonMap set of JSON properties
   * @return the converted Java object
   */
  public T convertFromMap(Map<String, Object> jsonMap, List<ColumnSchema> columns)
  {
    try {
      return objectMapper.convertValue(jsonMap, targetType);
    }
    catch (Exception e) {
      throw new IAE("Invalid input specification: " + e.getMessage());
    }
  }

  /**
   * Create a map of properties given a list of positional argument
   * values from Calcite. Null values in the list are ignored, others
   * converted to the map using the property ordering established in
   * the constructor.
   */
  public Map<String, Object> argsFromCalcite(List<Object> args)
  {
    Map<String, Object> argMap = new HashMap<>();
    int n = Math.min(properties.size(), args.size());
    for (int i = 0; i < n; i++) {
      Object value = args.get(i);
      if (value == null) {
        // Omitted named parameter: ignore
        continue;
      }
      argMap.put(properties.get(i).name(), value);
    }
    return argMap;
  }

  /**
   * Convert a map of encoded properties to a map of JSON fields.
   */
  private Map<String, Object> convertArgs(Map<String, ModelArg> args, List<ColumnSchema> columns)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    for (Map.Entry<String, ObjectConverter> entry : components.entrySet()) {
      ObjectConverter objConverter = entry.getValue();
      Map<String, Object> objMap = objConverter.convert(args, columns);
      if (objMap != null && !objMap.isEmpty()) {
        jsonMap.put(entry.getKey(), objMap);
      }
    }
    return jsonMap;
  }

  /**
   * Given a set of properties, verify that the properties are valid.
   *
   * @param properties the properties to check
   * @param columns the columns associated with the properties (required for
   * formats such as CSV)
   * @throws IAE if the properties are not valid
   */
  public void validateProperties(Map<String, Object> properties, List<ColumnSchema> columns)
  {
    // Crude-but-effective: just do the conversion
    convert(properties, columns);
  }

  /**
   * Given a set of properties, do an inverse conversion to a Calcite argument
   * list. Not used in production code, only in tests.
   */
  @VisibleForTesting
  public List<Object> toCalciteArgs(Map<String, Object> props)
  {
    return properties.stream()
        .map(prop -> props.get(prop.sqlName))
        .collect(Collectors.toList());
  }
}
