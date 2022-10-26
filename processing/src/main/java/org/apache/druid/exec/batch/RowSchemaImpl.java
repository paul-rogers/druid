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

package org.apache.druid.exec.batch;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Simple implementation of the row schema for rows with only (name, type) pairs.
 */
public class RowSchemaImpl implements RowSchema
{
  public static final RowSchema EMPTY_SCHEMA = new RowSchemaImpl(Collections.emptyList());

  public static class ColumnSchemaImpl implements ColumnSchema
  {
    private final String name;
    private final ColumnType type;

    public ColumnSchemaImpl(String name, ColumnType type)
    {
      this.name = name;
      this.type = type == null ? ColumnType.UNKNOWN_COMPLEX : type;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public ColumnType type()
    {
      return type;
    }

    @Override
    public String toString()
    {
      return StringUtils.format("%s %s", name, type);
    }

    @Override
    public boolean equals(Object o)
    {
      if (o == null || o.getClass() != getClass()) {
        return false;
      }
      ColumnSchemaImpl other = (ColumnSchemaImpl) o;
      return Objects.equals(name, other.name)
          && type == other.type;
    }

    @Override
    public int hashCode()
    {
      return Objects.hash(name, type);
    }
  }

  private final List<ColumnSchema> columns;
  private final Map<String, Integer> index = new HashMap<>();

  public RowSchemaImpl(List<ColumnSchema> columns)
  {
    this.columns = columns;
    for (int i = 0; i < columns.size(); i++) {
      ColumnSchema col = columns.get(i);
      if (index.put(col.name(), i) != null) {
        throw new ISE("Duplicate column name: %s", col.name());
      }
    }
  }

  @Override
  public int size()
  {
    return columns.size();
  }

  @Override
  public ColumnSchema column(String name)
  {
    Integer ordinal = index.get(name);
    return ordinal == null ? null : columns.get(ordinal);
  }

  @Override
  public ColumnSchema column(int ordinal)
  {
    return columns.get(ordinal);
  }

  @Override
  public int ordinal(String name)
  {
    Integer ordinal = index.get(name);
    return ordinal == null ? -1 : ordinal;
  }

  @Override
  public List<String> columnNames()
  {
    return columns.stream().map(col -> col.name()).collect(Collectors.toList());
  }

  @Override
  public String toString()
  {
    StringBuilder buf = new StringBuilder("(");
    for (int i = 0; i < size(); i++) {
      if (i > 0) {
        buf.append(", ");
      }
      buf.append(column(i).toString());
    }
    return buf.append(")").toString();
  }

  @Override
  public boolean equals(Object o)
  {
    if (o == null || o.getClass() != getClass()) {
      return false;
    }
    RowSchemaImpl other = (RowSchemaImpl) o;
    // No need to check index: it is derived from columns.
    return columns.equals(other.columns);
  }

  @Override
  public int hashCode()
  {
    return columns.hashCode();
  }
}
