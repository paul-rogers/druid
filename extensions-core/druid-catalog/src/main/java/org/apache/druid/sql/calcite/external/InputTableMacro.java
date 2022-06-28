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
import com.google.inject.Inject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.catalog.model.ExternalSpec;
import org.apache.druid.catalog.model.ExternalSpec.ExternalSpecConverter;
import org.apache.druid.catalog.model.ModelConverter;
import org.apache.druid.catalog.model.PropertyConverter;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.sql.calcite.table.DatasourceTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Used by {@link StagedOperatorConversion} to generate a {@link DatasourceTable}
 * that references an {@link ExternalDataSource}. This form uses name-based
 * arguments, and an internal name-to-JSON conversion mechanism to allow
 * simpler SQL. Schema comes from the Druid-specific EXTEND syntax, inspired
 * by Apache Phoenix support in Calcite, and added to Druid's version of the
 * Calcite parser.
 * <p>
 * Macro parameters are derived from the model used to convert
 * Calcite arguments to JSON fields.
 */
public class InputTableMacro implements TableMacro
{
  private final ObjectMapper jsonMapper;
  private final ModelConverter<ExternalSpec> modelConverter;

  @Inject
  public InputTableMacro(
      @Json final ObjectMapper jsonMapper,
      final ExternalSpecConverter model)
  {
    this.jsonMapper = jsonMapper;
    this.modelConverter = model;
  }

  /**
   * Create an external table from the Calcite arguments provided.
   * This form is used when no schema is provided:<pre><code>
   * SELECT ... FROM TABLE(fn(arg => value, ...))
   * </code></pre>
   */
  @Override
  public TranslatableTable apply(final List<Object> arguments)
  {
    return apply(
        modelConverter.argsFromCalcite(arguments),
        Collections.emptyList());
  }

  /**
   * Provide the underlying model converter which is needed by
   * wrapper macros to convert arguments into the form needed here.
   */
  public ModelConverter<ExternalSpec> model()
  {
    return modelConverter;
  }

  /**
   * Create a Druid external table with the named arguments and the
   * schema list provided. The schema list is in "model" format, to allow
   * use of this macro from two paths: the EXTERN path<pre><code>
   * SELECT ... FROM TABLE(fn(arg => value, ...)) (col1 <type1>, ...)
   * </code></pre>and the parameterized table path:<pre><code>
   * SELECT ... FROM TABLE(table(arg => value, ...))
   * </code></pre>
   */
  public TranslatableTable apply(Map<String, Object> arguments, List<ColumnSchema> schema)
  {
    return modelConverter
        .convert(arguments, schema)
        .toDruidTable(jsonMapper);
  }

  @Override
  public List<FunctionParameter> getParameters()
  {
    // Parameters are derived from the model converter which provides
    // the mapping from JSON fields to named SQL parameters. (Duplication is
    // allowed for fields in sibling subtypes). Calcite requires a fixed, known
    // parameters and types, so we define them in alphabetical order. Users are
    // NOT expected to use positional parameters, only parameters by name.
    List<FunctionParameter> params = new ArrayList<>();
    int ordinal = 0;
    for (PropertyConverter param : modelConverter.properties()) {
      params.add(new DynamicParam(ordinal++, param));
    }
    return params;
  }

  /**
   * Calcite functions typically define parameters statically: a function
   * takes a known set of arguments. For the external table, however, the
   * set of arguments depends on the set of format and source extensions
   * loaded at runtime, and so the table function signature differs
   * depending on these extensions. This dynamic parameter provides the
   * Calcite parameter based on the properties defined for the actual
   * set of extensions.
   * <p>
   * SQL code is stable across different extensions because the SQL code
   * uses named arguments, with most arguments optional. Thus, any given
   * SQL statement is bound only to the properties needed for the specific
   * source and format used by that one call.
   */
  private static class DynamicParam implements FunctionParameter
  {
    private int ordinal;
    private PropertyConverter prop;

    private DynamicParam(int ordinal, PropertyConverter prop)
    {
      this.ordinal = ordinal;
      this.prop = prop;
    }

    @Override
    public int getOrdinal()
    {
      return ordinal;
    }

    @Override
    public String getName()
    {
      return prop.name();
    }

    @Override
    public RelDataType getType(RelDataTypeFactory typeFactory)
    {
      return typeFactory.createJavaType(prop.sqlJavaType());
    }

    @Override
    public boolean isOptional()
    {
      // Perhaps required if used in staged(), optional if used
      // in a parameterized table. Since we can catch missing required
      // parameters during conversion, just always report the SQL
      // parameter as optional.
      return true;
    }
  }
}
