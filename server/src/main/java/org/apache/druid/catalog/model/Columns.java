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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.segment.column.ValueType;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Columns
{
  public static final String TIME_COLUMN = "__time";

  public static final String STRING = ValueType.STRING.name();
  public static final String LONG = ValueType.LONG.name();
  public static final String FLOAT = ValueType.FLOAT.name();
  public static final String DOUBLE = ValueType.DOUBLE.name();

  public static final String SQL_VARCHAR = "VARCHAR";
  public static final String SQL_BIGINT = "BIGINT";
  public static final String SQL_FLOAT = "FLOAT";
  public static final String SQL_DOUBLE = "DOUBLE";
  public static final String SQL_TIMESTAMP = "TIMESTAMP";


  public static final Set<String> NUMERIC_TYPES =
      ImmutableSet.of(LONG, FLOAT, DOUBLE);
  public static final Set<String> SCALAR_TYPES =
      ImmutableSet.of(STRING, LONG, FLOAT, DOUBLE);

  public static final Map<String, ColumnType> NAME_TO_DRUID_TYPES =
      new ImmutableMap.Builder<String, ColumnType>()
        .put(LONG, ColumnType.LONG)
        .put(FLOAT, ColumnType.FLOAT)
        .put(DOUBLE, ColumnType.DOUBLE)
        .put(STRING, ColumnType.STRING)
        .build();

  public static final Map<ColumnType, String> DRUID_TO_SQL_TYPES =
      new ImmutableMap.Builder<ColumnType, String>()
      .put(ColumnType.LONG, SQL_BIGINT)
      .put(ColumnType.FLOAT, SQL_FLOAT)
      .put(ColumnType.DOUBLE, SQL_DOUBLE)
      .put(ColumnType.STRING, SQL_VARCHAR)
      .build();

  private Columns()
  {
  }

  public static boolean isScalar(String type)
  {
    return SCALAR_TYPES.contains(StringUtils.toUpperCase(type.trim()));
  }

  public static ColumnType druidType(ColumnSpec spec)
  {
    if (isTimeColumn(spec.name())) {
      return ColumnType.LONG;
    }
    String dataType = spec.dataType();
    if (dataType == null) {
      return null;
    }
    return NAME_TO_DRUID_TYPES.get(StringUtils.toUpperCase(dataType));
  }

  public static String sqlType(ColumnSpec spec)
  {
    if (isTimeColumn(spec.name())) {
      return SQL_TIMESTAMP;
    }
    ColumnType druidType = druidType(spec);
    return druidType == null ? null : DRUID_TO_SQL_TYPES.get(druidType);
  }

  public static void validateScalarColumn(String name, String type)
  {
    if (type == null) {
      return;
    }
    if (!Columns.isScalar(type)) {
      throw new IAE("Not a supported Druid type: " + type);
    }
  }

  public static boolean isTimeColumn(String name)
  {
    return TIME_COLUMN.equals(name);
  }

  public static RowSignature convertSignature(List<ColumnSpec> columns)
  {
    RowSignature.Builder builder = RowSignature.builder();
    for (ColumnSpec col : columns) {
      ColumnType druidType = druidType(col);
      if (druidType == null) {
        druidType = ColumnType.STRING;
      }
      builder.add(col.name(), druidType);
    }
    return builder.build();
  }

  public static String sqlType(ColumnType druidType)
  {
    return DRUID_TO_SQL_TYPES.get(druidType);
  }
}
