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

package org.apache.druid.catalog.sql;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.SqlCall;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.model.table.TableFunction;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.external.CatalogExternalTableOperatorConversion.CatalogTableMacro;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.external.UserDefinedTableMacroFunction;
import org.apache.druid.sql.calcite.schema.AbstractTableSchema;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.table.DruidTable;

import javax.inject.Inject;

import java.lang.reflect.Type;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Represents the "ext" schema which holds the catalog external table
 * specifications which can be a "complete" table (the spec defines a
 * specific input), or, more likely, a "partial" table which is actually
 * a table function that requires parameters at query time.
 */
public class ExternalSchema extends AbstractTableSchema implements NamedSchema
{
  public static final int NOT_FETCHED = -1;
  public static final int UNDEFINED = 0;

  private final MetadataCatalog catalog;
  private final ObjectMapper jsonMapper;
  private final String schemaName = TableId.EXTERNAL_SCHEMA;

  @Inject
  public ExternalSchema(
      final MetadataCatalog catalog,
      @Json final ObjectMapper jsonMapper
  )
  {
    this.catalog = catalog;
    this.jsonMapper = jsonMapper;
  }

  @Override
  public String getSchemaName()
  {
    return schemaName;
  }

  @Override
  public Schema getSchema()
  {
    return this;
  }

  @Override
  public String getSchemaResourceType(String resourceName)
  {
    return ResourceType.EXTERNAL;
  }

  @Override
  public Set<String> getTableNames()
  {
    // There are no tables here: only functions. This could be refined
    // to check if an external table is complete, but doing so is not
    // really needed since this method, in Druid, is only ever called
    // from the information schema, where we also ask for function names.
    return Collections.emptySet();
  }

  @Override
  public Set<String> getFunctionNames()
  {
    return catalog.tableNames(schemaName);
  }

  /**
   * Resolve an external table from the catalog.
   */
  @Override
  public final Table getTable(String name)
  {
    ResolvedTable externalTable = getExternalTable(name);
    if (externalTable == null) {
      return null;
    }
    ExternalTableDefn defn = (ExternalTableDefn) externalTable.defn();
    TableFunction fn = defn.tableFn(externalTable);
    if (!fn.parameters().isEmpty()) {
      throw new IAE(
          "Table %s is a function: use TABLE(ext.%s(...)) and provide values for the parameters",
          name,
          name
      );
    }
    return toDruidTable(externalTable);
  }

  private ResolvedTable getExternalTable(String name)
  {
    TableId id = TableId.of(schemaName, name);
    ResolvedTable table = catalog.resolveTable(id);
    if (table == null || table.spec() == null) {
      return null;
    }
    if (!ExternalTableDefn.isExternalTable(table)) {
      throw new IAE("Table %s is not an external table", id.sqlName());
    }
    return table;
  }

  public DruidTable toDruidTable(ResolvedTable table)
  {
    ExternalTableDefn defn = (ExternalTableDefn) table.defn();
    ExternalTableSpec spec = defn.convert(table);
    return Externals.toExternalTable(spec, jsonMapper);
  }

  /**
   * Return the set of (table) functions for this schema. Here, those table
   * functions correspond to external tables. Function parameters, if any, are
   * defined by the "parameter" defined by each external table type. This form
   * allows the user to specify additional
   * properties in the query. Typically used to provide file names for a defined
   * input table that specifies directory location and properties.
   * <p>
   * Uses dynamic resolution so we only pull from the catalog those tables
   * which a query references (typically 0, since most queries don't reference
   * input tables.) If we used the default Calcite version, we'd have to provide
   * all catalog entries to every query so that Calcite could maintain the list
   * and do the name lookup.
   */
  @Override
  public Collection<Function> getFunctions(String name)
  {
    ResolvedTable inputTable = getExternalTable(name);
    if (inputTable == null) {
      return Collections.emptyList();
    }
    return Collections.singletonList(
      new ParameterizedTableMacro(name, inputTable)
    );
  }

  /**
   * Table macro "shim", created dynamically, to wrap a {@link ResolvedTable}
   * so we can merge catalog and function parameters.
   * <p>
   * This form is a Calcite {@code Function}, but not an operator (a
   * {@code SqlFunction}. That is, there is no outer {@code SqlFunction} for this
   * macro. Calcite will create an instance of {@code SqlUserDefinedTableMacro}
   * automatically, based on the fact that this function implements {@code TableMacro}.
   */
  public static class ParameterizedTableMacro
      extends CatalogTableMacro // Catalog-based table macro
      implements org.apache.calcite.schema.TableFunction, // For information schema metadata
      AuthorizableOperator // For Druid authorization
  {
    private final ResolvedTable externalTable;

    public ParameterizedTableMacro(
        final String tableName,
        final ResolvedTable externalTable
    )
    {
      super(tableName, externalTable);
      this.externalTable = externalTable;
    }

    @Override
    public Set<ResourceAction> computeResources(final SqlCall call)
    {
      return Collections.singleton(Externals.externalRead(name));
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory, List<Object> arguments)
    {
      // For a catalog external table, we only call this function for the information
      // schema, and only with an empty list of arguments. We return the defined schema;
      // we are not in a position to know the schema that might be given in an
      // individual query.
      return Externals.toRelDataType(externalTable);
    }

    @Override
    public Type getElementType(List<Object> arguments)
    {
      // Not used at present.
      throw new UOE("Not supported");
    }
  }
}
