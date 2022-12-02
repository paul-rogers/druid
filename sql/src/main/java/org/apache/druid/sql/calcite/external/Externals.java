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

package org.apache.druid.sql.calcite.external;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.table.ExternalTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Conversion functions to-from the SQL, catalog and MSQ representations
 * of external tables.
 */
public class Externals
{
  /**
   * Convert parameters from Catalog external table definition form to the SQL form
   * used for a table macro and its function.
   */
  public static List<FunctionParameter> convertParameters(TableFunction fn)
  {
    return convertToCalciteParameters(fn.parameters());
  }

  private static List<FunctionParameter> convertToCalciteParameters(List<TableFunction.ParameterDefn> paramDefns)
  {
    ImmutableList.Builder<FunctionParameter> params = ImmutableList.builder();
    for (int i = 0; i < paramDefns.size(); i++) {
      TableFunction.ParameterDefn paramDefn = paramDefns.get(i);
      params.add(new FunctionParameterImpl(
          i,
          paramDefn.name(),
          DruidTypeSystem.TYPE_FACTORY.createJavaType(paramDefn.type()),
          paramDefn.isOptional()
      ));
    }
    return params.build();
  }

  /**
   * Extract the data types (only) from a list of SQL parameters.
   */
  public static List<RelDataType> dataTypes(List<FunctionParameter> parameters)
  {
    return parameters
        .stream()
        .map(parameter -> parameter.getType(DruidTypeSystem.TYPE_FACTORY))
        .collect(Collectors.toList());
  }

  /**
   * Define a variadic (variable arity) type checker that allows an argument
   * count that ranges from the number of required parameters to the number of
   * available parameters. We have to define this because the Calcite form does
   * not allow optional parameters, but we allow any parameter to be optional.
   * We are also not fussy about the type: we catch any type errors from the
   * declared types. We catch missing required parameters at conversion time,
   * where we also catch invalid values, incompatible values, and so on.
   */
  public static SqlOperandTypeChecker variadic(List<FunctionParameter> params)
  {
    int min = 0;
    for (FunctionParameter param : params) {
      if (!param.isOptional()) {
        min++;
      }
    }
    SqlOperandCountRange range = SqlOperandCountRanges.between(min, params.size());
    return new SqlOperandTypeChecker()
    {
      @Override
      public boolean checkOperandTypes(
          SqlCallBinding callBinding,
          boolean throwOnFailure)
      {
        return range.isValidCount(callBinding.getOperandCount());
      }

      @Override
      public SqlOperandCountRange getOperandCountRange()
      {
        return range;
      }

      @Override
      public String getAllowedSignatures(SqlOperator op, String opName)
      {
        return opName + "(...)";
      }

      @Override
      public boolean isOptional(int i)
      {
        return true;
      }

      @Override
      public Consistency getConsistency()
      {
        return Consistency.NONE;
      }
    };
  }

  public static Map<String, Object> convertArguments(
      final TableFunction fn,
      final List<Object> arguments
  )
  {
    List<TableFunction.ParameterDefn> params = fn.parameters();
    ImmutableMap.Builder<String, Object> builder = ImmutableMap.builder();
    for (int i = 0; i < arguments.size(); i++) {
      builder.put(params.get(i).name(), arguments.get(i));
    }
    return builder.build();
  }

  // Converts from a list of (identifier, type, ...) pairs to
  // list of column specs. The schema itself comes from the
  // Druid-specific EXTEND syntax added to the parser.
  public static List<ColumnSpec> convertColumns(SqlNodeList schema)
  {
    List<ColumnSpec> columns = new ArrayList<>();
    for (int i = 0; i < schema.size(); i += 2) {
      final String name = convertName((SqlIdentifier) schema.get(i));
      String sqlType = convertType(name, (SqlDataTypeSpec) schema.get(i + 1));
      columns.add(new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, name, sqlType, null));
    }
    return columns;
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
   * Define the SQL input column type from a type provided in the
   * EXTEND clause. Calcite allows any form of type. But, Druid
   * requires only the Druid supported types (and their aliases.)
   * <p>
   * Druid has its own rules for nullability. We ignore any nullability
   * clause in the EXTEND list.
   */
  private static String convertType(String name, SqlDataTypeSpec dataType)
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
      return SqlTypeName.VARCHAR.name();
    }
    if (SqlTypeName.INT_TYPES.contains(type)) {
      return SqlTypeName.BIGINT.name();
    }
    switch (type) {
      case DOUBLE:
        return SqlType.DOUBLE.name();
      case FLOAT:
      case REAL:
        return SqlType.FLOAT.name();
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
   * Create an MSQ ExternalTable given an external table spec. Enforces type restructions
   * (which should be revisited.)
   */
  public static ExternalTable buildExternalTable(ExternalTableSpec spec, ObjectMapper jsonMapper)
  {
    // Prevent a RowSignature that has a ColumnSignature with name "__time" and type that is not LONG because it
    // will be automatically cast to LONG while processing in RowBasedColumnSelectorFactory.
    // This can cause an issue when the incorrectly type-casted data is ingested or processed upon. One such example
    // of inconsistency is that functions such as TIME_PARSE evaluate incorrectly
    //
    // TODO: Fix the underlying problem: we should not make assumptions about the input
    // data, nor restrict the form of that data.
    Optional<ColumnType> timestampColumnTypeOptional = spec.signature.getColumnType(ColumnHolder.TIME_COLUMN_NAME);
    if (timestampColumnTypeOptional.isPresent() && !timestampColumnTypeOptional.get().equals(ColumnType.LONG)) {
      throw new ISE("EXTERN function with __time column can be used when __time column is of type long. "
                    + "Please change the column name to something other than __time");
    }

    return toExternalTable(spec, jsonMapper);
  }

  public static ResourceAction externalRead(String name)
  {
    return new ResourceAction(new Resource(name, ResourceType.EXTERNAL), Action.READ);
  }

  public static ExternalTable toExternalTable(ExternalTableSpec spec, ObjectMapper jsonMapper)
  {
    return new ExternalTable(
        new ExternalDataSource(
            spec.inputSource,
            spec.inputFormat,
            spec.signature
          ),
        spec.signature,
        jsonMapper
    );
  }
}
