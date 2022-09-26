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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class FieldTypes
{
  public static class FieldTypeDefn<T>
  {
    public final String typeName;
    public final Class<T> valueClass;
    public final TypeReference<T> valueType;

    public FieldTypeDefn(
        final String typeName,
        final Class<T> valueClass,
        final TypeReference<T> valueType
    )
    {
      this.typeName = Preconditions.checkNotNull(typeName);
      Preconditions.checkArgument(valueClass != null || valueType != null);
      this.valueClass = valueClass;
      this.valueType = valueType;
    }

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

    /**
     * Convert a value provided as a SQL function argument into the canonical form
     * stored in a spec.
     */
    public T decodeSqlValue(ObjectMapper mapper, Object value)
    {
      return decode(mapper, value);
    }
  }

  public static class StringListType extends FieldTypeDefn<List<String>>
  {
    public StringListType()
    {
      super("string list",
          null,
          new TypeReference<List<String>>() {}
      );
    }

    /**
     * VARCHAR-to-List&lt;String> conversion. The string can contain zero items,
     * one items, or a list. The list items are separated by a comma and optional
     * whitespace.
     */
    @Override
    public List<String> decodeSqlValue(ObjectMapper mapper, Object value)
    {
      if (value == null) {
        return null;
      }
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      return Arrays.asList(((String) value).split(",\\s*"));
    }
  }

  // Types used to create Java objects
  public static final FieldTypeDefn<String> STRING_TYPE = new FieldTypeDefn<>("string", String.class, null);
  public static final FieldTypeDefn<Integer> INT_TYPE = new FieldTypeDefn<>("integer", Integer.class, null);
  public static final FieldTypeDefn<Boolean> BOOLEAN_TYPE = new FieldTypeDefn<>("Boolean", Boolean.class, null);
  public static final FieldTypeDefn<List<String>> STRING_LIST_TYPE = new StringListType();

  // Types used as spec fields, but not for direct-to-Java conversions.
  public static final FieldTypeDefn<List<ClusterKeySpec>> CLUSTER_KEY_LIST_TYPE =
      new FieldTypeDefn<List<ClusterKeySpec>>(
          "cluster key list",
          null,
          new TypeReference<List<ClusterKeySpec>>() { }
      );
}
