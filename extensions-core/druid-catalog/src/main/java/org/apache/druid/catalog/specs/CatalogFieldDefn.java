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

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.client.util.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.catalog.Columns;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.PeriodGranularity;
import org.joda.time.Period;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class CatalogFieldDefn<T>
{
  public static class FieldSerDeType<T>
  {
    public final Class<T> valueClass;
    public final TypeReference<T> valueType;

    public FieldSerDeType(
        final Class<T> valueClass,
        final TypeReference<T> valueType
    )
    {
      Preconditions.checkArgument(valueClass != null || valueType != null);
      this.valueClass = valueClass;
      this.valueType = valueType;
    }

    public T decode(ObjectMapper mapper, Object value)
    {
      if (value == null) {
        return null;
      }
      if (valueClass != null) {
        return mapper.convertValue(value, valueClass);
      } else {
        return mapper.convertValue(value, valueType);
      }
    }
  }

  public static class StringFieldDefn extends CatalogFieldDefn<String>
  {
    public StringFieldDefn(String name)
    {
      super(
          name,
          "String",
          new FieldSerDeType<>(String.class, null)
      );
    }
  }

  public static class GranularityFieldDefn extends StringFieldDefn
  {
    public GranularityFieldDefn(String name)
    {
      super(name);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      String gran = decode(value, jsonMapper);
      validateGranularity(gran);
    }

    public void validateGranularity(String value)
    {
      if (value == null) {
        return;
      }
      try {
        new PeriodGranularity(new Period(value), null, null);
      }
      catch (IllegalArgumentException e) {
        throw new IAE(StringUtils.format("[%s] is an invalid granularity string", value));
      }
    }
  }

  public static class SegmentGranularityFieldDefn extends GranularityFieldDefn
  {
    public SegmentGranularityFieldDefn()
    {
      super(Constants.SEGMENT_GRANULARITY_FIELD);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      String gran = decode(value, jsonMapper);
      if (Strings.isNullOrEmpty(gran)) {
        throw new IAE("Segment granularity is required.");
      }
      validateGranularity(gran);
    }
  }

  public static class IntFieldDefn extends CatalogFieldDefn<Integer>
  {
    public IntFieldDefn(String name)
    {
      super(
          name,
          "Integer",
          new FieldSerDeType<>(Integer.class, null)
      );
    }
  }

  public static class ListFieldDefn<T> extends CatalogFieldDefn<List<T>>
  {
    public ListFieldDefn(String name, String elementType, TypeReference<List<T>> typeRef)
    {
      super(
          name,
          "List<" + elementType + ">",
          new FieldSerDeType<>(null, typeRef)
      );
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<T> merge(Object existing, Object updates)
    {
      if (updates == null) {
        return (List<T>) existing;
      }
      if (existing == null) {
        return (List<T>) updates;
      }
      List<T> existingList;
      List<T> updatesList;
      try {
        existingList = (List<T>) existing;
        updatesList = (List<T>) updates;
      }
      catch (ClassCastException e) {
        throw new IAE(
            "Value of field %s must be a list, found %s",
            name(),
            updates.getClass().getSimpleName()
        );
      }
      Set<T> existingSet = new HashSet<>(existingList);
      List<T> revised = new ArrayList<>(existingList);
      for (T col : updatesList) {
        if (!existingSet.contains(col)) {
          revised.add(col);
        }
      }
      return revised;
    }
  }

  public static class HiddenColumnsDefn extends ListFieldDefn<String>
  {
    public HiddenColumnsDefn()
    {
      super(
          Constants.HIDDEN_COLUMNS_FIELD,
          "String",
          new TypeReference<List<String>>() {}
      );
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return;
      }
      List<String> hiddenColumns = decode(value, jsonMapper);
      for (String col : hiddenColumns) {
        if (Columns.TIME_COLUMN.equals(col)) {
          throw new IAE(
              StringUtils.format("Cannot hide column %s", col)
          );
        }
      }
    }
  }

  private final String name;
  private final String typeName;
  private final CatalogFieldDefn.FieldSerDeType<T> serDeType;

  public CatalogFieldDefn(String name, String typeName, CatalogFieldDefn.FieldSerDeType<T> serDeType)
  {
    this.name = name;
    this.typeName = typeName;
    this.serDeType = serDeType;
  }

  public String name()
  {
    return name;
  }

  public String typeName()
  {
    return typeName;
  }

  public CatalogFieldDefn.FieldSerDeType<T> serDeType()
  {
    return serDeType;
  }

  public T decode(Object value, ObjectMapper jsonMapper)
  {
    try {
      return serDeType.decode(jsonMapper, value);
    }
    catch (Exception e) {
      throw new IAE("Value [%s] is not valid for property [%s]", value, name);
    }
  }

  public void validate(Object value, ObjectMapper jsonMapper)
  {
    decode(value, jsonMapper);
  }

  @SuppressWarnings("unchecked")
  public T merge(Object current, Object update)
  {
    return (T) (update == null ? current : update);
  }
}