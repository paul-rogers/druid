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

package org.apache.druid.sql.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.Parameterized.ParameterDefn;
import org.apache.druid.catalog.model.table.ExternalSpec;
import org.apache.druid.catalog.model.table.InputTableDefn;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.ExternalTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.SqlTypeName;

public class CatalogConversions
{
  public static DruidTable toDruidTable(ExternalSpec externSpec, ObjectMapper jsonMapper)
  {
    return toDruidTable(externSpec, externSpec.signature(), jsonMapper);
  }

  public static DruidTable toDruidTable(ExternalSpec externSpec, RowSignature sig, ObjectMapper jsonMapper)
  {
    return new ExternalTable(
        new ExternalDataSource(
            externSpec.inputSource(),
            externSpec.inputFormat(),
            sig
          ),
        sig,
        jsonMapper
    );
  }

  public static ColumnSpec toColumnSchema(SqlIdentifier ident, SqlDataTypeSpec dataType)
  {
    return new ColumnSpec(
        InputTableDefn.INPUT_COLUMN_TYPE,
        convertName(ident),
        convertSqlType(ident.getSimple(), dataType),
        null
    );
  }

  /**
   * Define the Druid input schema from a name provided in the EXTEND
   * clause. Calcite allows any form of name: a.b.c, say. But, Druid
   * requires only simple names: "a", or "x".
   */
  private static String convertName(SqlIdentifier ident)
  {
    if (!ident.isSimple()) {
      throw new IAE(StringUtils.format(
          "Column [%s] must have a simple name",
          ident));
    }
    return ident.getSimple();
  }

  /**
   * Define the Druid input column type from a type provided in the
   * EXTEND clause. Calcite allows any form of type. But, Druid
   * requires only the Druid supported types (and their aliases.)
   * <p>
   * Druid has its own rules for nullabilty. We ignore any nullability
   * clause in the EXTEND list.
   */
  private static String convertSqlType(String name, SqlDataTypeSpec dataType)
  {
    SqlTypeNameSpec spec = dataType.getTypeNameSpec();
    if (spec == null) {
      throw unsupportedType(name, dataType);
    }
    SqlIdentifier typeName = spec.getTypeName();
    if (typeName == null || !typeName.isSimple()) {
      throw unsupportedType(name, dataType);
    }
    SqlTypeName type = SqlTypeName.get(typeName.getSimple());
    if (type == null) {
      throw unsupportedType(name, dataType);
    }
    if (SqlTypeName.CHAR_TYPES.contains(type)) {
      return Columns.VARCHAR;
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      return Columns.BIGINT;
    }
    switch (type) {
      case DOUBLE:
        return Columns.DOUBLE;
      case FLOAT:
      case REAL:
        return Columns.FLOAT;
      default:
        throw unsupportedType(name, dataType);
    }
  }

  /**
   * Define the Druid input column type from a type provided in the
   * EXTEND clause. Calcite allows any form of type. But, Druid
   * requires only the Druid supported types (and their aliases.)
   * <p>
   * Druid has its own rules for nullabilty. We ignore any nullability
   * clause in the EXTEND list.
   */
  private static ColumnType convertType(String name, SqlDataTypeSpec dataType)
  {
    SqlTypeNameSpec spec = dataType.getTypeNameSpec();
    if (spec == null) {
      throw unsupportedType(name, dataType);
    }
    SqlIdentifier typeName = spec.getTypeName();
    if (typeName == null || !typeName.isSimple()) {
      throw unsupportedType(name, dataType);
    }
    SqlTypeName type = SqlTypeName.get(typeName.getSimple());
    if (type == null) {
      throw unsupportedType(name, dataType);
    }
    if (SqlTypeName.CHAR_TYPES.contains(type)) {
      return ColumnType.STRING;
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      return ColumnType.LONG;
    }
    switch (type) {
      case DOUBLE:
        return ColumnType.DOUBLE;
      case FLOAT:
      case REAL:
        return ColumnType.FLOAT;
      default:
        throw unsupportedType(name, dataType);
    }
  }

  private static RuntimeException unsupportedType(String name, SqlDataTypeSpec dataType)
  {
    return new IAE(StringUtils.format(
        "Column [%s] has an unsupported type: [%s]",
        name,
        dataType));
  }

  /**
   * Create a map of properties given a list of positional argument
   * values from Calcite. Null values in the list are ignored, others
   * converted to the map using the property ordering established in
   * the constructor.
   */
  public static Map<String, Object> argsFromCalcite(
      final List<ParameterDefn> parameters,
      final List<Object> args
  )
  {
    Map<String, Object> argMap = new HashMap<>();
    int n = Math.min(parameters.size(), args.size());
    for (int i = 0; i < n; i++) {
      Object value = args.get(i);
      if (value == null) {
        // Omitted named parameter: ignore
        continue;
      }
      argMap.put(parameters.get(i).name(), value);
    }
    return argMap;
  }
}
