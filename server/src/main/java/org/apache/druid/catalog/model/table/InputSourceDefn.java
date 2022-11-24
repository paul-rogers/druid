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

package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnDefn;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ModelProperties;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;
import org.apache.druid.catalog.model.ParameterizedDefn;
import org.apache.druid.catalog.model.PropertyAttributes;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.InputFormats.InputFormatDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 * <p>
 * An input source can be thought of as a "connection", though Druid does not
 * use that term. An input source is a template for an external table. The input
 * source says how to get data, and optionally the format and structure of that data.
 * Since Druid never ingests the same data twice, the actual external table needs
 * details that says which data to read on any specific ingestion. Thus, an input
 * source is a "partial table": all the information that remains constant
 * across ingestions, but without the information that changes. The changing
 * information is typically the list of files (or objects or URLs) to ingest.
 * <p>
 * The pattern is:<br>
 * {@code input source + parameters --> external table}
 * <p>
 * Since an input source is a parameterized (partial) external table, we can reuse
 * the table metadata structures and APIs, avoiding the need to have a separate (but
 * otherwise identical) structure for input sources.
 * <p>
 * The input source implements the mechanism for parameterized tables,
 * but does not implement the {@link ParameterizedDefn} interface itself.
 * Input sources which are parameterized implement that interface to expose
 * methods defined here.
 */
public abstract class InputSourceDefn extends TableDefn
{
  public static final String EXTERNAL_COLUMN_TYPE = "extern";

  public abstract static class FormattedInputSourceDefn extends InputSourceDefn
  {
    public static final String FORMAT_PROPERTY = "format";

    private final Map<String, InputFormatDefn> formats;

    public FormattedInputSourceDefn(
        final String name,
        final String typeValue,
        final List<PropertyDefn<?>> properties,
        final List<ColumnDefn> columnDefns,
        final List<InputFormatDefn> formats
    )
    {
      super(
          name,
          typeValue,
          addFormatProperties(properties, formats),
          columnDefns
      );
      final ImmutableMap.Builder<String, InputFormatDefn> builder = ImmutableMap.builder();
      for (InputFormatDefn format : formats) {
        builder.put(format.typeTag(), format);
      }
      this.formats = builder.build();
    }

    /**
     * Add format properties to the base set, in the order of the formats,
     * in the order defined by the format. Allow same-named properties across
     * formats, as long as the types are the same.
     */
    private static List<PropertyDefn<?>> addFormatProperties(
        final List<PropertyDefn<?>> properties,
        final List<InputFormatDefn> formats
    )
    {
      final List<PropertyDefn<?>> toAdd = new ArrayList<>();
      final PropertyDefn<?> formatProp = new ModelProperties.StringPropertyDefn(FORMAT_PROPERTY, PropertyAttributes.SQL_FN_PARAM);
      toAdd.add(formatProp);
      final Map<String, PropertyDefn<?>> formatProps = new HashMap<>();
      for (InputFormatDefn format : formats) {
        for (PropertyDefn<?> prop : format.properties()) {
          final PropertyDefn<?> existing = formatProps.putIfAbsent(prop.name(), prop);
          if (existing == null) {
            toAdd.add(prop);
          } else if (existing.getClass() != prop.getClass()) {
            throw new ISE(
                "Format %s, property %s of class %s conflicts with another format property of class %s",
                format.name(),
                prop.name(),
                prop.getClass().getSimpleName(),
                existing.getClass().getSimpleName()
            );
          }
        }
      }
      return CatalogUtils.concatLists(properties, toAdd);
    }

    @Override
    protected InputFormat convertFormat(ResolvedTable table)
    {
      return formatDefn(table).convert(table);
    }

    protected InputFormatDefn formatDefn(ResolvedTable table)
    {
      final String formatTag = table.stringProperty(FORMAT_PROPERTY);
      if (formatTag == null) {
        throw new IAE("%s property must be set", FORMAT_PROPERTY);
      }
      final InputFormatDefn formatDefn = formats.get(formatTag);
      if (formatDefn == null) {
        throw new IAE(
            "Format type [%s] for property %s is not valid",
            formatTag,
            FORMAT_PROPERTY
        );
      }
      return formatDefn;
    }

    @Override
    public void validate(ResolvedTable table)
    {
      super.validate(table);
      formatDefn(table).validate(table);
      List<ColumnSpec> columns = table.spec().columns();
      if (columns == null || columns.isEmpty()) {
        throw new IAE(
            "An external table of type %s must specify one or more columns",
            table.spec().type()
        );
      }
    }
  }

  /**
   * Definition of a column in a detail (non-rollup) datasource.
   */
  public static class ExternalColumnDefn extends ColumnDefn
  {
    public ExternalColumnDefn()
    {
      super(
          "Column",
          EXTERNAL_COLUMN_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  protected static final ExternalColumnDefn INPUT_COLUMN_DEFN = new ExternalColumnDefn();

  private final List<PropertyDefn<?>> fields;

  public InputSourceDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn<?>> fields,
      final List<ColumnDefn> columnDefns
  )
  {
    super(name, typeValue, fields, columnDefns);
    this.fields = fields;
  }

  public List<PropertyDefn<?>> parameters()
  {
    return fields.stream()
        .filter(f -> PropertyAttributes.isExternTableParameter(f))
        .collect(Collectors.toList());
  }

  public List<PropertyDefn<?>> tableFunctionParameters()
  {
    return fields.stream()
        .filter(f -> PropertyAttributes.isSqlFunctionParameter(f))
        .collect(Collectors.toList());
  }

  /**
   * Merge parameters provided by a SQL table function with the catalog information
   * provided in the resolved table to produce a new resolved table used for a
   * specific query.
   */
  public abstract ResolvedTable mergeParameters(ResolvedTable table, Map<String, Object> values);

  public ExternalTableSpec convertToExtern(ResolvedTable table)
  {
    return new ExternalTableSpec(
        convertSource(table),
        convertFormat(table),
        Columns.convertSignature(table.spec())
    );
  }

  /**
   * Convert a resolved table to the Druid internal {@link InputSource}
   * object required by an MSQ query.
   */
  protected abstract InputSource convertSource(ResolvedTable table);

  /**
   * Convert a resolved table to the Druid internal {@link InputFormat}
   * object required by an MSQ query. Not all input sources require a format.
   */
  protected InputFormat convertFormat(ResolvedTable table)
  {
    return null;
  }

  protected InputSource convertObject(
      final ObjectMapper jsonMapper,
      final Map<String, Object> jsonMap,
      final Class<? extends InputSource> targetClass
  )
  {
    try {
      return jsonMapper.convertValue(jsonMap, targetClass);
    }
    catch (Exception e) {
      throw new IAE(e, "Invalid table specification");
    }
  }

  public ExternalTableSpec applyParameters(ResolvedTable table, Map<String, Object> parameters)
  {
    return convertToExtern(mergeParameters(table, parameters));
  }

  public static boolean isExternalTable(ResolvedTable table)
  {
    return table.defn() instanceof InputSourceDefn;
  }

  public static Set<String> tableTypes()
  {
    // Known input tables. Get this from a registry later.
    return ImmutableSet.of(
        InlineInputSourceDefn.TABLE_TYPE,
        HttpInputSourceDefn.TABLE_TYPE,
        LocalInputSourceDefn.TABLE_TYPE
    );
  }

  public static RowSignature rowSignature(TableSpec spec)
  {
    final RowSignature.Builder builder = RowSignature.builder();
    if (spec.columns() != null) {
      for (ColumnSpec col : spec.columns()) {
        ColumnType druidType = Columns.SQL_TO_DRUID_TYPES.get(StringUtils.toUpperCase(col.sqlType()));
        if (druidType == null) {
          druidType = ColumnType.STRING;
        }
        builder.add(col.name(), druidType);
      }
    }
    return builder.build();
  }
}
