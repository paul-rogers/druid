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

import org.apache.curator.shaded.com.google.common.collect.Lists;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.QueryContexts;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;

/**
 * Converts one SQL argument to a JSON property. Provides the SQL-to-JSON
 * name mapping, along with type validation or conversion.
 */
public class PropertyConverter
{
  /**
   * Handles SQL-to-JSON type conversion.
   */
  public interface TypeConverter<T>
  {
    Class<?> sqlJavaType();
    T convert(Object value);
  }

  /**
   * VARCHAR-to-String conversion.
   */
  protected static final TypeConverter<String> VARCHAR_TYPE = new TypeConverter<String>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return String.class;
    }

    @Override
    public String convert(Object value)
    {
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      return (String) value;
    }
  };

  /**
   * VARCHAR-to-List&lt;String> conversion. The string can contain zero items,
   * one items, or a list. The list items are separated by a comma and optional
   * whitespace.
   */
  protected static final TypeConverter<List<String>> VARCHAR_LIST_TYPE = new TypeConverter<List<String>>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return String.class;
    }

    @Override
    public List<String> convert(Object value)
    {
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      String[] values = ((String) value).split(",\\s*");
      return Lists.newArrayList(values);
    }
  };

  /**
   * VARCHAR-to-List&lt;File> conversion for the local file input source.
   * List semantics are the same as for {@link #VARCHAR_LIST_TYPE}.
   */
  protected static final TypeConverter<List<File>> VARCHAR_FILE_LIST_TYPE = new TypeConverter<List<File>>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return String.class;
    }

    @Override
    public List<File> convert(Object value)
    {
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      String[] values = ((String) value).split(",\\s*");
      List<File> files = new ArrayList<>();
      for (String strValue : values) {
        files.add(new File(strValue));
      }
      return files;
    }
  };

  /**
   * VARCHAR-to-List&lt;URI> conversion for the HTTP input source.
   * List semantics are the same as for {@link #VARCHAR_LIST_TYPE}.
   */
  protected static final TypeConverter<List<URI>> VARCHAR_URI_LIST_TYPE = new TypeConverter<List<URI>>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return String.class;
    }

    @Override
    public List<URI> convert(Object value)
    {
      if (!(value instanceof String)) {
        throw new IAE(StringUtils.format("Argument [%s] is not a VARCHAR", value));
      }
      String[] values = ((String) value).split(",\\s*");
      List<URI> uris = new ArrayList<>();
      for (String strValue : values) {
        try {
          uris.add(new URI(strValue));
        }
        catch (URISyntaxException e) {
          throw new IAE(StringUtils.format("Argument [%s] is not a valid URI", value));
        }
      }
      return uris;
    }
  };

  /**
   * BOOLEAN conversion. The SQL value can be a string with the values "true"
   * or "false", or a Java Boolean type.
   */
  protected static final TypeConverter<Boolean> BOOLEAN_TYPE = new TypeConverter<Boolean>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return Boolean.class;
    }

    @Override
    public Boolean convert(Object value)
    {
      try {
        // TODO: Allow case-insensitive conversion.
        return QueryContexts.getAsBoolean("x", value, false);
      }
      catch (IAE e) {
        throw new IAE(StringUtils.format("Argument [%s] is not a BOOLEAN", value));
      }
    }
  };

  /**
   * INTEGER type. The value must be an Integer or a String with a
   * valid integer value.
   */
  protected static final TypeConverter<Integer> INT_TYPE = new TypeConverter<Integer>()
  {
    @Override
    public Class<?> sqlJavaType()
    {
      return Integer.class;
    }

    @Override
    public Integer convert(Object value)
    {
      try {
        return QueryContexts.getAsInt("x", value, 0);
      }
      // The above throws NumberFormatException for non-numbers.
      // Should probably be handed in the above method.
      catch (Exception e) {
        throw new IAE(StringUtils.format("Argument [%s] is not an INT", value));
      }
    }
  };

  final String sqlName;
  final PropertyConverter.TypeConverter<?> sqlType;
  final String jsonPath;
  final boolean required;

  public PropertyConverter(
      final String sqlName,
      final String jsonPath,
      final PropertyConverter.TypeConverter<?> sqlType,
      final boolean required)
  {
    this.sqlName = sqlName;
    this.sqlType = sqlType;
    this.jsonPath = jsonPath;
    this.required = required;
  }

  public PropertyConverter(
      final String sqlName,
      final String jsonPath,
      final PropertyConverter.TypeConverter<?> sqlType)
  {
    this(sqlName, jsonPath, sqlType, false);
  }

  public String name()
  {
    return sqlName;
  }

  public Class<?> sqlJavaType()
  {
    return sqlType.sqlJavaType();
  }

  public boolean required()
  {
    return required;
  }
}
