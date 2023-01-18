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

package org.apache.druid.exec.util;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Ordering;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Holds type information that should be in {@code ColumnType}, but isn't.
 */
public class TypeRegistry
{
  public interface TypeAttributes
  {
    ColumnType type();
    boolean comperable();
    Ordering<Object> objectOrdering();
  }

  private static class TypeAttribsImpl implements TypeAttributes
  {
    private final ColumnType type;
    private final Ordering<Object> objectOrdering;

    @SuppressWarnings("unchecked")
    public TypeAttribsImpl(ColumnType type, @SuppressWarnings("rawtypes") Ordering objectOrdering)
    {
      this.type = type;
      this.objectOrdering = objectOrdering;
    }

    @Override
    public ColumnType type()
    {
      return type;
    }

    @Override
    public boolean comperable()
    {
      return objectOrdering != null;
    }

    @Override
    public Ordering<Object> objectOrdering()
    {
      return objectOrdering;
    }
  }

  public static final TypeRegistry INSTANCE = new TypeRegistry();

  private final Map<ColumnType, TypeAttributes> types;

  public TypeRegistry()
  {
    List<TypeAttribsImpl> types = new ArrayList<>();
    types.add(new TypeAttribsImpl(ColumnType.STRING, (Ordering<?>) Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.LONG, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.FLOAT, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.DOUBLE, Ordering.natural()));
    types.add(new TypeAttribsImpl(ColumnType.UNKNOWN_COMPLEX, null));

    ImmutableMap.Builder<ColumnType, TypeAttributes> builder = ImmutableMap.builder();
    for (TypeAttribsImpl type : types) {
      builder.put(type.type(), type);
    }
    this.types = builder.build();
  }

  public TypeAttributes resolve(ColumnType type)
  {
    return types.get(type);
  }

  public Comparator<Object> sortOrdering(SortColumn key, ColumnType type)
  {
    Ordering<Object> ordering = ordering(key.columnName(), type);
    if (key.descending()) {
      ordering = ordering.reverse();
    }
    return ordering;
  }

  public Comparator<Object>[] sortOrdering(List<SortColumn> keys, RowSchema schema)
  {
    Comparator<Object>[] comparators = makeComparators(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      SortColumn key = keys.get(i);
      ColumnSchema col = schema.column(key.columnName());
      if (col == null) {
        throw new ISE("Sort key [%s] not found in the input schema", key.columnName());
      }
      comparators[i] = sortOrdering(key, col.type());
    }
    return comparators;
  }

  // Just to suppress the warning.
  @SuppressWarnings("unchecked")
  private static Comparator<Object>[] makeComparators(int n)
  {
    return new Comparator[n];
  }

  public Comparator<Object>[] ordering(List<String> keys, RowSchema schema)
  {
    Comparator<Object>[] comparators = makeComparators(keys.size());
    for (int i = 0; i < keys.size(); i++) {
      String key = keys.get(i);
      ColumnSchema col = schema.column(key);
      if (col == null) {
        throw new ISE("Key [%s] not found in the input schema", key);
      }
      comparators[i] = ordering(key, col.type());
    }
    return comparators;
  }

  public Ordering<Object> ordering(String key, ColumnType type)
  {
    if (type == null) {
      throw new ISE("Sort key [%s]: input schema has no type", key);
    }
    TypeAttributes attribs = resolve(type);
    if (attribs == null) {
      throw new ISE(
          "Sort key [%s]: type [%s] not found in the type registry",
          key,
          type.asTypeString()
      );
    }
    Ordering<Object> ordering = attribs.objectOrdering();
    if (ordering == null) {
      throw new ISE(
          "Sort key [%s]: type [%s] is not orderable",
          key,
          type.asTypeString()
      );
    }
    return ordering;
  }
}
