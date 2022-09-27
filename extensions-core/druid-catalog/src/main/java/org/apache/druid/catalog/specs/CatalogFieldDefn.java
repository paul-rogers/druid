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
import com.google.common.base.Strings;
import org.apache.druid.catalog.specs.table.Constants;
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
  public static class StringFieldDefn extends CatalogFieldDefn<String>
  {
    public StringFieldDefn(String name)
    {
      super(name, FieldTypes.STRING_TYPE);
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

  public static class IntFieldDefn extends CatalogFieldDefn<Integer>
  {
    public IntFieldDefn(String name)
    {
      super(name, FieldTypes.INT_TYPE);
    }
  }

  public static class BooleanFieldDefn extends CatalogFieldDefn<Boolean>
  {
    public BooleanFieldDefn(String name)
    {
      super(name, FieldTypes.BOOLEAN_TYPE);
    }
  }

  public static class ListFieldDefn<T> extends CatalogFieldDefn<List<T>>
  {
    public ListFieldDefn(
        final String name,
        final FieldTypes.FieldTypeDefn<List<T>> fieldType
    )
    {
      super(name, fieldType);
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

  public static class StringListDefn extends ListFieldDefn<String>
  {
    public StringListDefn(String name)
    {
      super(
          name,
          FieldTypes.STRING_LIST_TYPE
      );
    }
  }

  private final String name;
  private final FieldTypes.FieldTypeDefn<T> fieldType;

  public CatalogFieldDefn(
      final String name,
      final FieldTypes.FieldTypeDefn<T> fieldType
  )
  {
    this.name = name;
    this.fieldType = fieldType;
  }

  public String name()
  {
    return name;
  }

  public FieldTypes.FieldTypeDefn<T> type()
  {
    return fieldType;
  }

  public T decode(Object value, ObjectMapper jsonMapper)
  {
    try {
      return fieldType.decode(jsonMapper, value);
    }
    catch (Exception e) {
      throw new IAE(
          "Value [%s] is not valid for property [%s], expected %s",
          value,
          name,
          fieldType.typeName()
      );
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

  @Override
  public String toString()
  {
    return getClass().getSimpleName() + "{"
        + "name: " + name
        + "type:" + fieldType
        + "}";
  }
}
