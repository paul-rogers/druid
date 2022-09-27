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

package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.catalog.specs.FieldTypes.FieldTypeDefn;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;

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
public interface JsonObjectConverter<T>
{
  public static class JsonProperty
  {
    protected final String jsonKey;
    protected final PropertyDefn<?> fieldDefn;

    public JsonProperty(
        final String jsonKey,
        final PropertyDefn<?> fieldDefn
    )
    {
      this.jsonKey = jsonKey;
      this.fieldDefn = fieldDefn;
    }

    public String propertyKey()
    {
      return fieldDefn.name();
    }

    public String jsonKey()
    {
      return jsonKey;
    }

    public FieldTypeDefn<?> type()
    {
      return fieldDefn.type();
    }

    /**
     * Convert a value in canonical form to the form required to create a Java
     * object using Jackson deserialization.
     */
    public Object encodeToJava(Object value)
    {
      return value;
    }
  }

  public interface JsonSubclassConverter<T> extends JsonObjectConverter<T>
  {
    String typePropertyValue();
    T convert(ResolvedTable table, String jsonTypeFieldName);
  }

  public static class JsonSubclassConverterImpl<T> extends JsonObjectConverterImpl<T> implements JsonSubclassConverter<T>
  {
    private final String typePropertyValue;
    private final String jsonTypeValue;

    public JsonSubclassConverterImpl(
        final String typePropertyValue,
        final String jsonTypeValue,
        final List<JsonProperty> properties,
        final Class<T> targetClass
    )
    {
      super(properties, targetClass);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(typePropertyValue));
      Preconditions.checkArgument(!Strings.isNullOrEmpty(jsonTypeValue));
      this.typePropertyValue = typePropertyValue;
      this.jsonTypeValue = jsonTypeValue;
    }

    @Override
    public T convert(ResolvedTable table, String jsonTypeFieldName)
    {
      Map<String, Object> fields = gatherFields(table);
      fields.put(jsonTypeFieldName, jsonTypeValue);
      return convertFromMap(fields, table.jsonMapper());
    }

    @Override
    public String typePropertyValue()
    {
      return typePropertyValue;
    }
  }

  public static class JsonObjectConverterImpl<T> implements JsonObjectConverter<T>
  {

    private final List<JsonProperty> properties;
    private final Class<T> targetClass;

    public JsonObjectConverterImpl(
        final List<JsonProperty> properties,
        final Class<T> targetClass
    )
    {
      this.properties = properties;
      this.targetClass = targetClass;
      validateProperties();
    }

    @Override
    public T convert(ResolvedTable table)
    {
      return convertFromMap(gatherFields(table), table.jsonMapper());
    }

    public Map<String, Object> gatherFields(ResolvedTable table)
    {
      Map<String, Object> jsonMap = new HashMap<>();
      for (JsonProperty prop : properties) {
        Object value = table.property(prop.propertyKey());
        if (value == null) {
          continue;
        }
        try {
          Object convertedValue = prop.encodeToJava(value);
          if (jsonMap.put(prop.jsonKey(), convertedValue) != null) {
            List<String> overloads = getOverloads(prop.jsonKey());
            throw new IAE(StringUtils.format(
                "Specify only one of %s",
                overloads
                ));
          }
        }
        catch (IAE e) {
          throw new IAE(StringUtils.format(
              "The value [%s] is not valid for argument %s of type %s: %s",
              value,
              prop.propertyKey(),
              prop.type().typeName(),
              e.getMessage()));
        }
      }
      return jsonMap;
    }

    public T convertFromMap(Map<String, Object> jsonMap, ObjectMapper jsonMapper)
    {
      try {
        return jsonMapper.convertValue(jsonMap, targetClass);
      }
      catch (Exception e) {
        throw new IAE(e, "Invalid input specification");
      }
    }

    private List<String> getOverloads(String jsonPath)
    {
      List<String> overloads = new ArrayList<>();
      for (JsonProperty prop : properties) {
        if (prop.jsonKey().equals(jsonPath)) {
          overloads.add(prop.propertyKey());
        }
      }
      return overloads;
    }

    private void validateProperties()
    {
      Map<String, JsonProperty> propertyMap = new HashMap<>();
      for (JsonProperty prop : properties) {
        if (propertyMap.put(prop.propertyKey(), prop) != null) {
          throw new ISE(
              "Property %s is duplicated for type %s",
              prop.propertyKey(),
              targetClass.getSimpleName()
          );
        }
      }
      propertyMap = new HashMap<>();
      for (JsonProperty prop : properties) {
        JsonProperty existing = propertyMap.get(prop.jsonKey());
        if (existing == null) {
          propertyMap.put(prop.jsonKey(), prop);
        } else if (existing.type() != prop.type()) {
          throw new ISE(
              "Json property [%s] defined with conflicting types: [%s] and [%s]",
              prop.jsonKey(),
              prop.type().typeName(),
              existing.type().typeName()
          );
        }
      }
    }
  }

  public static class JsonObjectConverterFieldType<T> implements FieldTypeDefn<T>
  {
    public foo(JsonObjectConverter<T> converter)
    {
      this.converter = converter;
    }

    @Override
    public String typeName()
    {
      // TODO Auto-generated method stub
      return converter.typeName();
    }

    @Override
    public T decode(ObjectMapper mapper, Object value)
    {
      // TODO Auto-generated method stub
      return null;
    }

  }

  public T convert(ResolvedTable table);
}
