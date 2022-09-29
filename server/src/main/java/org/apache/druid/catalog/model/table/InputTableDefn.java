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
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnDefn;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.Parameterized;
import org.apache.druid.catalog.model.Parameterized.ParameterDefn;
import org.apache.druid.catalog.model.Properties.PropertyDefn;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefn;
import org.apache.druid.catalog.model.table.InputFormats.InputFormatDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 * <p>
 * The input table implements the mechanism for parameterized tables,
 * but does not implement the {@link Parameterized} interface itself.
 * Tables which are parameterized implement that interface to expose
 * methods defined here.
 */
public abstract class InputTableDefn extends TableDefn
{
  public static final String INPUT_COLUMN_TYPE = "input";

  public abstract static class FormattedInputTableDefn extends InputTableDefn
  {
    public static final String INPUT_FORMAT_PROPERTY = "format";

    private Map<String, InputFormatDefn> formats;

    public FormattedInputTableDefn(
        final String name,
        final String typeValue,
        final List<PropertyDefn> properties,
        final List<ColumnDefn> columnDefns,
        final List<InputFormatDefn> formats,
        final List<ParameterDefn> parameters
    )
    {
      super(
          name,
          typeValue,
          addFormatProperties(properties, formats),
          columnDefns,
          parameters
      );
      ImmutableMap.Builder<String, InputFormatDefn> builder = ImmutableMap.builder();
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
    private static List<PropertyDefn> addFormatProperties(
        final List<PropertyDefn> properties,
        final List<InputFormatDefn> formats
    )
    {
      List<PropertyDefn> toAdd = new ArrayList<>();
      Map<String, PropertyDefn> formatProps = new HashMap<>();
      for (InputFormatDefn format : formats) {
        for (PropertyDefn prop : format.properties()) {
          PropertyDefn existing = formatProps.putIfAbsent(prop.name(), prop);
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
      String formatTag = table.stringProperty(INPUT_FORMAT_PROPERTY);
      if (formatTag == null) {
        throw new IAE("%s property must be set", INPUT_FORMAT_PROPERTY);
      }
      InputFormatDefn formatDefn = formats.get(formatTag);
      if (formatDefn == null) {
        throw new IAE(
            "Format type [%s] for property %s is not valid",
            formatTag,
            INPUT_FORMAT_PROPERTY
        );
      }
      return formatDefn;
    }

    @Override
    public void validate(ResolvedTable table)
    {
      super.validate(table);
      formatDefn(table).validate(table);
    }
  }

  /**
   * Definition of a column in a detail (non-rollup) datasource.
   */
  public static class InputColumnDefn extends ColumnDefn
  {
    public InputColumnDefn()
    {
      super(
          "Column",
          INPUT_COLUMN_TYPE,
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

  protected static final InputColumnDefn INPUT_COLUMN_DEFN = new InputColumnDefn();
  private final List<ParameterDefn> parameterList;
  private final Map<String, ParameterDefn> parameterMap;


  public InputTableDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn> fields,
      final List<ColumnDefn> columnDefns,
      final List<ParameterDefn> parameters
  )
  {
    super(name, typeValue, fields, columnDefns);
    if (CollectionUtils.isNullOrEmpty(parameters)) {
      this.parameterMap = null;
      this.parameterList = null;
    } else {
      this.parameterList = parameters;
      Map<String, ParameterDefn> params = new HashMap<>();
      for (ParameterDefn param : parameters) {
        if (params.put(param.name(), param) != null) {
          throw new ISE("Duplicate parameter: ", param.name());
        }
      }
      this.parameterMap = ImmutableMap.copyOf(params);
    }
  }

  public List<ParameterDefn> parameters()
  {
    return parameterList;
  }

  public ParameterDefn parameter(String key)
  {
    return parameterMap.get(key);
  }

  public abstract ResolvedTable mergeParameters(ResolvedTable table, Map<String, Object> values);

  public ExternalSpec convertToExtern(ResolvedTable table)
  {
    return new ExternalSpec(
        convertSource(table),
        convertFormat(table),
        Columns.convertSignature(table.spec())
    );
  }

  protected InputFormat convertFormat(ResolvedTable table)
  {
    return null;
  }

  protected abstract InputSource convertSource(ResolvedTable table);

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

  public ExternalSpec applyParameters(ResolvedTable table, Map<String, Object> parameters)
  {
    ResolvedTable revised = mergeParameters(table, parameters);
    return convertToExtern(revised);
  }

  public static boolean isInputTable(ResolvedTable table)
  {
    return table.defn() instanceof InputTableDefn;
  }

  public static Set<String> tableTypes()
  {
    // Known input tables. Get this from a registry later.
    return CatalogUtils.setOf(
        InlineTableDefn.TABLE_TYPE,
        HttpTableDefn.TABLE_TYPE,
        LocalTableDefn.TABLE_TYPE
    );
  }
}
