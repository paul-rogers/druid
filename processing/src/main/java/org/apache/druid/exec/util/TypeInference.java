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

import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.segment.column.ColumnType;

import java.util.List;
import java.util.Map;

/**
 * Experimental tool to discover the schema of a list of objects or maps.
 * Infers the types by sampling. Created because the scan query provides
 * column names but not types: better to push the schema creation all the
 * way back to the segment to properly capture types.
 */
public class TypeInference
{
  public static RowSchema untypedSchema(List<String> columnNames)
  {
    SchemaBuilder builder = new SchemaBuilder();
    for (String name : columnNames) {
      builder.scalar(name, null);
    }
    return builder.build();
  }

  public static RowSchema inferMapSchema(List<Map<String, Object>> rows, List<String> columnNames)
  {
    SchemaBuilder builder = new SchemaBuilder();
    for (String name : columnNames) {
      builder.scalar(name, inferMapColumnType(rows, name));
    }
    return builder.build();
  }

  public static RowSchema inferArraySchema(List<Object[]> rows, List<String> columnNames)
  {
    SchemaBuilder builder = new SchemaBuilder();
    for (int i = 0; i < columnNames.size(); i++) {
      String name = columnNames.get(i);
      builder.scalar(name, inferArrayColumnType(rows, i));
    }
    return builder.build();
  }

  public static ColumnType inferMapColumnType(List<Map<String, Object>> rows, String name)
  {
    for (Map<String, Object> row : rows) {
      ColumnType type = inferType(row.get(name));
      if (type != null) {
        return type;
      }
    }
    return null;
  }

  public static ColumnType inferArrayColumnType(List<Object[]> rows, int index)
  {
    for (Object[] row : rows) {
      ColumnType type = inferType(row[index]);
      if (type != null) {
        return type;
      }
    }
    return null;
  }

  public static ColumnType inferType(Object value)
  {
    if (value == null) {
      return null;
    }
    if (value instanceof String) {
      return ColumnType.STRING;
    }
    if (value instanceof Long) {
      return ColumnType.LONG;
    }
    if (value instanceof Double) {
      return ColumnType.DOUBLE;
    }
    if (value instanceof Float) {
      return ColumnType.FLOAT;
    }
    // TODO: Infer array types
    return ColumnType.UNKNOWN_COMPLEX;
  }
}
