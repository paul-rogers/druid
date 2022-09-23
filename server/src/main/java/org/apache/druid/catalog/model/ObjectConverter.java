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

import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Converts a list of table function arguments to a JSON map that
 * corresponds to properties for a subclass of some Java object.
 * Each object has a SQL argument that gives the type, which
 * corresponds to the subclass. Each subclass gives the corresponding
 * field and type to use in JSON.
 * <p>
 * For example, the input format might be given by the "format" argument
 * in SQL, which maps to a subclass of {@code InputFormat}, each of which
 * is identified by a Jackson-defined type for some Jackson-defined property.
 * For example, the type field is often "type", and for the {@code CsvInputFormat}
 * the value is "csv."
 * <p>
 * The SQL argument for the type typically has a different name than the
 * Jackson property, and the type values may also be different.
 * <p>
 * The subtype, once identified, picks out the arguments that are for that type.
 * The list can contain values for other objects (such as the input source.)
 * The argument names must be distinct in SQL, but different Java objects may
 * reuse the same JSON properties.
 */
public class ObjectConverter
{
  protected final String jsonKey;
  private final String typeArgName;
  private final String typeArgKey;
  private final Map<String, SubTypeConverter> subTypes;

  public ObjectConverter(
      final String jsonKey,
      final String typeArgName,
      final String typeArgKey,
      final SubTypeConverter[] subTypes)
  {
    this.jsonKey = jsonKey;
    this.typeArgName = typeArgName;
    this.typeArgKey = typeArgKey;
    this.subTypes = new HashMap<>();
    for (SubTypeConverter subType : subTypes) {
      if (this.subTypes.put(subType.sqlValue, subType) != null) {
        throw new ISE(StringUtils.format(
            "Subtype %s is duplicated for object %s",
            subType.sqlValue,
            typeArgName));
      }
    }
  }

  public Map<String, Object> convert(Map<String, ModelArg> args, List<ColumnSchema> columns)
  {
    ModelArg typeArg = args.get(typeArgName);
    if (typeArg == null) {
      return null;
    }
    typeArg.consume();
    String typeKey = typeArg.asString();
    SubTypeConverter subTypeConverter = subTypes.get(typeKey);
    if (subTypeConverter == null) {
      throw new IAE(StringUtils.format(
          "The value '%s' is not valid type key for argument %s",
          typeKey,
          typeArgName));
    }
    Map<String, Object> jsonMap = subTypeConverter.convert(args, columns);
    if (jsonMap == null) {
      jsonMap = new HashMap<>();
    }
    jsonMap.put(typeArgKey, subTypeConverter.jsonTypeValue);
    return jsonMap;
  }

  public void gatherProperties(Map<String, PropertyConverter> propertyMap)
  {
    for (SubTypeConverter subType : subTypes.values()) {
      subType.gatherProperties(propertyMap);
    }
    PropertyConverter dummyProp = new PropertyConverter(
        typeArgName,
        "",
        PropertyConverter.VARCHAR_TYPE,
        true);
    if (propertyMap.put(typeArgName, dummyProp) != null) {
      throw new ISE(StringUtils.format(
          "Property [%s] is both a property and a type key",
          typeArgName));
    }
  }
}
