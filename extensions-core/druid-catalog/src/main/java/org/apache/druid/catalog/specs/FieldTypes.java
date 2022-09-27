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

import java.util.List;

public class FieldTypes
{
  public interface FieldTypeDefn<T>
  {
    String typeName();
    T decode(ObjectMapper mapper, Object value);
  }

  public static class FieldClassDefn<T> implements FieldTypeDefn<T>
  {
    public final Class<T> valueClass;

    public FieldClassDefn(
        final Class<T> valueClass
    )
    {
      this.valueClass = valueClass;
    }

    @Override
    public String typeName()
    {
      return valueClass.getSimpleName();
    }

    /**
     * Convert the value from the deserialized JSON format to the type
     * required by this field data type. Also used to decode values from
     * SQL parameters. As a side effect, verifies that the value is of
     * the correct type.
     */
    @Override
    public T decode(ObjectMapper mapper, Object value)
    {
      if (value == null) {
        return null;
      }
      return mapper.convertValue(value, valueClass);
    }

    @Override
    public String toString()
    {
      return typeName();
    }
  }

  public static class FieldTypeRefDefn<T> implements FieldTypeDefn<T>
  {
    public final String typeName;
    public final TypeReference<T> valueType;

    public FieldTypeRefDefn(
        final String typeName,
        final TypeReference<T> valueType
    )
    {
      this.typeName = Preconditions.checkNotNull(typeName);
      this.valueType = valueType;
    }

    @Override
    public String typeName()
    {
      return typeName;
    }

    /**
     * Convert the value from the deserialized JSON format to the type
     * required by this field data type. Also used to decode values from
     * SQL parameters. As a side effect, verifies that the value is of
     * the correct type.
     */
    @Override
    public T decode(ObjectMapper mapper, Object value)
    {
      if (value == null) {
        return null;
      }
      return mapper.convertValue(value, valueType);
    }

    @Override
    public String toString()
    {
      return typeName();
    }
  }

  public static class StringListType extends FieldTypeRefDefn<List<String>>
  {
    public StringListType()
    {
      super(
          "string list",
          new TypeReference<List<String>>() {}
      );
    }
  }

  // Types used to create Java objects
  public static final FieldTypeDefn<String> STRING_TYPE = new FieldClassDefn<>(String.class);
  public static final FieldTypeDefn<Integer> INT_TYPE = new FieldClassDefn<>(Integer.class);
  public static final FieldTypeDefn<Boolean> BOOLEAN_TYPE = new FieldClassDefn<>(Boolean.class);
  public static final FieldTypeDefn<List<String>> STRING_LIST_TYPE = new StringListType();

  // Types used as spec fields, but not for direct-to-Java conversions.
  public static final FieldTypeDefn<List<ClusterKeySpec>> CLUSTER_KEY_LIST_TYPE =
      new FieldTypeRefDefn<List<ClusterKeySpec>>(
          "cluster key list",
          new TypeReference<List<ClusterKeySpec>>() { }
      );
}
