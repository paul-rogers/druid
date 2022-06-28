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

package org.apache.druid.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;

import java.util.Map;
import java.util.Set;

public class Columns
{
  public static final String TIME_COLUMN = "__time";

  public static final String VARCHAR = "VARCHAR";
  public static final String BIGINT = "BIGINT";
  public static final String FLOAT = "FLOAT";
  public static final String DOUBLE = "DOUBLE";
  public static final String TIMESTAMP = "TIMESTAMP";

  public static final Set<String> NUMERIC_TYPES = ImmutableSet.of(BIGINT, FLOAT, DOUBLE);
  public static final Set<String> SCALAR_TYPES = ImmutableSet.of(VARCHAR, BIGINT, FLOAT, DOUBLE);

  public static final Map<String, ColumnType> SQL_TO_DRUID_TYPES =
      new ImmutableMap.Builder<String, ColumnType>()
        .put(BIGINT, ColumnType.LONG)
        .put(FLOAT, ColumnType.FLOAT)
        .put(DOUBLE, ColumnType.DOUBLE)
        .put(VARCHAR, ColumnType.STRING)
        .build();

  public static boolean isTimestamp(String type)
  {
    return TIMESTAMP.equalsIgnoreCase(type.trim());
  }

  public static boolean isScalar(String type)
  {
    return SCALAR_TYPES.contains(StringUtils.toUpperCase(type.trim()));
  }

  public static ColumnType druidType(String sqlType)
  {
    if (sqlType == null) {
      return null;
    }
    return SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(sqlType));
  }

  public static void validateScalarColumn(String name, String type)
  {
    if (type == null) {
      return;
    }
    if (Columns.TIME_COLUMN.equals(name)) {
      if (!Columns.isTimestamp(type)) {
        throw new IAE("__time column must have type TIMESTAMP");
      }
    } else if (!Columns.isScalar(type)) {
      throw new IAE("Not a supported SQL type: " + type);
    }
  }

  public static boolean isTimeColumn(String name)
  {
    return TIME_COLUMN.equals(name);
  }
}
