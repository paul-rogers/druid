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
import com.google.inject.Inject;
import org.apache.calcite.avatica.SqlType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCallBinding;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlOperandTypeChecker;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedTableMacro;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.PropertyAttributes;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.HttpTableDefn;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.expression.DruidExpression;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.external.UserDefinedTableMacroFunction.ExtendedTableMacro;
import org.apache.druid.sql.calcite.planner.DruidTypeSystem;
import org.apache.druid.sql.calcite.planner.PlannerContext;
import org.apache.druid.sql.calcite.table.ExternalTable;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class HttpOperatorConversion implements SqlOperatorConversion
{
  public static final String FUNCTION_NAME = "http";
  private final SqlUserDefinedTableMacro operator;

  @Inject
  public HttpOperatorConversion(
      final TableDefnRegistry registry,
      @Json final ObjectMapper jsonMapper
  )
  {
    ExternalTableDefn tableDefn = (ExternalTableDefn) registry.defnFor(HttpTableDefn.TABLE_TYPE);
    this.operator = new HttpOperator(new HttpTableMacro(tableDefn, jsonMapper));
  }

  @Override
  public SqlOperator calciteOperator()
  {
    return operator;
  }

  @Nullable
  @Override
  public DruidExpression toDruidExpression(PlannerContext plannerContext, RowSignature rowSignature, RexNode rexNode)
  {
    return null;
  }

  private static class HttpOperator extends UserDefinedTableMacroFunction implements AuthorizableOperator
  {
    public HttpOperator(final HttpTableMacro macro)
    {
      super(
          new SqlIdentifier(FUNCTION_NAME, SqlParserPos.ZERO),
          ReturnTypes.CURSOR,
          null,
          // Use our own definition of variadic since Calcite's doesn't allow
          // optional parameters.
          variadic(macro.parameters),
          macro.dataTypes(),
          macro
      );
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(ExternalOperatorConversion.EXTERNAL_RESOURCE_ACTION);
    }
  }

  public static class HttpTableMacro implements ExtendedTableMacro
  {
    private final List<FunctionParameter> parameters;
    private final ExternalTableDefn tableDefn;
    private final ObjectMapper jsonMapper;

    public HttpTableMacro(final ExternalTableDefn tableDefn, final ObjectMapper jsonMapper)
    {
      this.tableDefn = tableDefn;
      this.jsonMapper = jsonMapper;
      this.parameters = convertParameters(tableDefn);
    }

    public static List<FunctionParameter> convertParameters(final ExternalTableDefn tableDefn)
    {
      List<ModelProperties.PropertyDefn<?>> props = tableDefn.tableFunctionParameters();
      ImmutableList.Builder<FunctionParameter> params = ImmutableList.builder();
      for (int i = 0; i < props.size(); i++) {
        ModelProperties.PropertyDefn<?> prop = props.get(i);
        params.add(new FunctionParameterImpl(
            i,
            prop.name(),
            DruidTypeSystem.TYPE_FACTORY.createJavaType(PropertyAttributes.sqlParameterType(prop)),
            PropertyAttributes.isOptional(prop)
        ));
      }
      return params.build();
    }

    public List<RelDataType> dataTypes()
    {
      return parameters
          .stream()
          .map(parameter -> parameter.getType(DruidTypeSystem.TYPE_FACTORY))
          .collect(Collectors.toList());
    }

    @Override
    public TranslatableTable apply(final List<Object> arguments)
    {
      throw new IAE(
          "The %s table function requires an EXTEND clause with a schema.",
          FUNCTION_NAME
      );
    }

    @Override
    public TranslatableTable apply(List<Object> arguments, SqlNodeList schema)
    {
      final TableBuilder builder = TableBuilder.of(tableDefn);
      for (int i = 0; i < parameters.size(); i++) {
        String name = parameters.get(i).getName();
        Object value = arguments.get(i);
        if (value == null) {
          continue;
        }
        PropertyDefn<?> prop = tableDefn.property(name);
        builder.property(name, prop.decodeSqlValue(value, jsonMapper));
      }

      // Converts from a list of (identifier, type, ...) pairs to
      // a Druid row signature. The schema itself comes from the
      // Druid-specific EXTEND syntax added to the parser.
      for (int i = 0; i < schema.size(); i += 2) {
        final String name = convertName((SqlIdentifier) schema.get(i));
        String sqlType = convertType(name, (SqlDataTypeSpec) schema.get(i + 1));
        builder.column(name, sqlType);
      }
      ResolvedTable table = builder.buildResolved(jsonMapper);
      ExternalTableSpec externSpec = tableDefn.convertToExtern(table);

      // Prevent a RowSignature that has a ColumnSignature with name "__time" and type that is not LONG because it
      // will be automatically cast to LONG while processing in RowBasedColumnSelectorFactory.
      // This can cause an issue when the incorrectly type-casted data is ingested or processed upon. One such example
      // of inconsistency is that functions such as TIME_PARSE evaluate incorrectly
      Optional<ColumnType> timestampColumnTypeOptional = externSpec.signature.getColumnType(ColumnHolder.TIME_COLUMN_NAME);
      if (timestampColumnTypeOptional.isPresent() && !timestampColumnTypeOptional.get().equals(ColumnType.LONG)) {
        throw new ISE("EXTERN function with __time column can be used when __time column is of type long. "
                      + "Please change the column name to something other than __time");
      }

      return new ExternalTable(
            new ExternalDataSource(externSpec.inputSource, externSpec.inputFormat, externSpec.signature),
            externSpec.signature,
            jsonMapper
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

    @Override
    public List<FunctionParameter> getParameters()
    {
      return parameters;
    }
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
}
