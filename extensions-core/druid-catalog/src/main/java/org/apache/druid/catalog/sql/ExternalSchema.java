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
import com.google.common.collect.ImmutableList;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.DruidSimpleCalciteSchema;
import org.apache.calcite.jdbc.SchemaCreator;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.table.ExternalTableDefn;
import org.apache.druid.catalog.model.table.ExternalTableSpec;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.guice.annotations.Json;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.Externals;
import org.apache.druid.sql.calcite.schema.AbstractTableSchema;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.ExternalTable;

import javax.inject.Inject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Custom input-focused implementation of the cached metadata catalog.
 * Caches the {@link DatasourceTable} objects which Calcite uses. This works
 * because a @{code DruidTable} is immutable.
 * <p>
 * Uses the same update mechanism as the general cache, but ignores
 * updates for all but the input schema.
 */
public class ExternalSchema extends AbstractTableSchema implements NamedSchema, SchemaCreator
{
  public static final int NOT_FETCHED = -1;
  public static final int UNDEFINED = 0;

  // Temporary: we may want to add a real resource.
  public static final String EXTERNAL_RESOURCE = "EXTERNAL";

  static {
    ResourceType.registerResourceType(EXTERNAL_RESOURCE);
  }

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
    return EXTERNAL_RESOURCE;
  }

  @Override
  public Set<String> getTableNames()
  {
    return catalog.tableNames(schemaName);
  }

  /**
   * Resolve an external table from the catalog.
   */
  @Override
  public final Table getTable(String name)
  {
    ResolvedTable inputTable = getExternalTable(name);
    if (inputTable == null) {
      return null;
    }
    return toDruidTable(inputTable);
  }

  private ResolvedTable getExternalTable(String name)
  {
    TableId id = TableId.of(schemaName, name);
    ResolvedTable table = catalog.resolveTable(id);
    if (table == null || table.spec() == null) {
      return null;
    }
    if (!ExternalTableDefn.isExternalTable(table)) {
      throw new IAE(
          StringUtils.format("Table [%s] is not an input table", id.sqlName()));
    }
    return table;
  }

  public DruidTable toDruidTable(ResolvedTable table)
  {
    ExternalTableDefn defn = (ExternalTableDefn) table.defn();
    ExternalTableSpec spec = defn.convertToExtern(table);
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

  /**
   * Create a custom form of {@link CalciteSchema} that dynamically creates
   * a function to wrap an input table, allowing the user to specify additional
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
  public CalciteSchema createSchema(CalciteSchema parentSchema, String name)
  {
    return new DruidSimpleCalciteSchema(parentSchema, this, name)
    {
      @Override
      protected void addImplicitFunctionsToBuilder(
          ImmutableList.Builder<Function> builder,
          String name,
          boolean caseSensitive
      )
      {
        ResolvedTable inputTable = getExternalTable(name);
        if (inputTable != null) {
          builder.add(new ParameterizedTableMacro(inputTable));
        }
      }
    };
  }

  /**
   * Table macro "shim", created dynamically, to wrap an {@link InputTableMacro}
   * with an {@link InputTableSpec} so we can merge catalog and function parameters.
   * <p>
   * This form is a Calcite {@code Function}, but not an operator (a
   * {@code SqlFunction}. That is, there is no outer {@code SqlFunction} for this
   * macro. Calcite will create an instance of {@code SqlUserDefinedTableMacro}
   * automatically, based on the fact that this function implements {@code TableMacro}.
   */
  public static class ParameterizedTableMacro implements TableMacro
  {
    private final List<FunctionParameter> parameters;
    private final ResolvedTable externalTable;

    public ParameterizedTableMacro(
        ResolvedTable externalTable
    )
    {
      this.externalTable = externalTable;
      this.parameters = Externals.convertTableParameters((ExternalTableDefn) externalTable.defn());
    }

    @Override
    public TranslatableTable apply(List<Object> arguments)
    {
      Map<String, Object> args = new HashMap<>();
      List<FunctionParameter> parameters = getParameters();
      ExternalTableDefn tableDefn = (ExternalTableDefn) externalTable.defn();
      ObjectMapper jsonMapper = externalTable.jsonMapper();
      for (int i = 0; i < parameters.size(); i++) {
        String name = parameters.get(i).getName();
        Object value = arguments.get(i);
        if (value == null) {
          continue;
        }
        PropertyDefn<?> prop = tableDefn.property(name);
        args.put(name, prop.decodeSqlValue(value, jsonMapper));
      }
      final ExternalTableSpec externSpec = tableDefn.applyParameters(externalTable, args);
      return Externals.buildExternalTable(externSpec, jsonMapper);
    }

    @Override
    public List<FunctionParameter> getParameters()
    {
      return parameters;
    }
  }
}
