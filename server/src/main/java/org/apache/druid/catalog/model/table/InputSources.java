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
import com.google.common.base.Strings;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.BaseFunctionDefn.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class InputSources
{
  /**
   * Base class for input source definitions.
   *
   * @see {@link FormattedInputSourceDefn} for the base class for (most) input formats
   * which take an input format.
   */
  public abstract static class BaseInputSourceDefn implements InputSourceDefn
  {
    /**
     * The "from-scratch" table function for this input source. The parameters
     * are those defined by the subclass, and the apply simply turns around and
     * asks the input source definition to do the conversion.
     */
    public class FullTableFunction extends BaseFunctionDefn
    {
      public FullTableFunction(List<ParameterDefn> parameters)
      {
        super(parameters);
      }

      @Override
      public ExternalTableSpec apply(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
      {
        return adHocTable(args, columns, jsonMapper);
      }
    }

    /**
     * The one and only from-scratch table function for this input source. The
     * function is defined a bind time, not construction time, since it typically
     * needs visibility to the set of available input formats.
     */
    private FullTableFunction fullTableFn;

    @Override
    public void bind(TableDefnRegistry registry)
    {
      this.fullTableFn = defineFullTableFunction();
    }

    /**
     * Overridden by each subclass to define the parameters needed by each
     * input source.
     */
    protected abstract FullTableFunction defineFullTableFunction();

    @Override
    public TableFunction externFn()
    {
      return fullTableFn;
    }

    /**
     * Overridden by each subclass to return the input source class to be
     * used for JSON conversions.
     */
    protected abstract Class<? extends InputSource> inputSourceClass();

    @Override
    public void validate(ResolvedExternalTable table)
    {
      convertSource(table);
    }

    /**
     * Converts the input source given in a table spec. Since Druid input sources
     * were not designed for the use by the catalog or SQL, some cleanup is done to
     * simplify the parameters which the user provides.
     *
     * @param table the resolved external table spec
     * @return the input source converted from the spec
     */
    protected InputSource convertSource(ResolvedExternalTable table)
    {
      Map<String, Object> jsonMap = new HashMap<>(table.sourceMap());
      auditTableProperties(jsonMap);
      return convert(jsonMap, table.resolvedTable().jsonMapper());
    }

    /**
     * Optional step to audit or adjust the input source properties prior to
     * conversion via Jackson. Changes are made directly in the {@code jsonMap}.
     */
    protected void auditTableProperties(Map<String, Object> jsonMap)
    {
    }

    @Override
    public ExternalTableSpec convertTable(ResolvedExternalTable table)
    {
      return new ExternalTableSpec(
          convertSource(table),
          convertFormat(table),
          Columns.convertSignature(table.resolvedTable().spec().columns())
      );
    }

    /**
     * Convert the format spec, if any, to an input format.
     */
    protected abstract InputFormat convertFormat(ResolvedExternalTable table);

    public InputSource convert(
        final Map<String, Object> jsonMap,
        final ObjectMapper jsonMapper
    )
    {
      try {
        return jsonMapper.convertValue(jsonMap, inputSourceClass());
      }
      catch (Exception e) {
        throw new IAE(e, "Invalid input source specification");
      }
    }

    /**
     * Define a table "from scratch" using SQL function arguments.
     */
    protected ExternalTableSpec adHocTable(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      auditTableProperties(args);
      return new ExternalTableSpec(
          convertAdHocSource(args, jsonMapper),
          convertAdHocFormat(args, columns, jsonMapper),
          Columns.convertSignature(columns)
      );
    }

    /**
     * Convert the input source using arguments to a "from scratch" table function.
     */
    protected InputSource convertAdHocSource(Map<String, Object> args, ObjectMapper jsonMapper)
    {
      Map<String, Object> jsonMap = new HashMap<>();
      convertAdHocArgs(jsonMap, args);
      auditTableProperties(jsonMap);
      return convert(jsonMap, jsonMapper);
    }

    /**
     * Convert SQL arguments to the corresponding "generic JSON" form in the given map.
     * The map will then be adjusted and converted to the actual input source.
     */
    protected abstract void convertAdHocArgs(Map<String, Object> jsonMap, Map<String, Object> args);

    /**
     * Convert SQL arguments, and the column schema, to an input format, if required.
     */
    protected InputFormat convertAdHocFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      return null;
    }
  }

  /**
   * Base class for input formats that require an input format (which is most of them.)
   * By default, an input source supports all formats defined in the table registry, but
   * specific input sources can be more restrictive. The list of formats defines the list
   * of SQL function arguments available when defining a table from scratch.
   */
  public abstract static class FormattedInputSourceDefn extends BaseInputSourceDefn
  {
    public static final String FORMAT_PROPERTY = "format";

    private Map<String, InputFormatDefn> formats;

    @Override
    public void bind(TableDefnRegistry registry)
    {
      formats = registry.formats();
      super.bind(registry);
    }

    @Override
    protected FullTableFunction defineFullTableFunction()
    {
      List<ParameterDefn> fullTableParams = fullTableFnParameters();
      List<ParameterDefn> allParams = addFormatParameters(fullTableParams);
      return new FullTableFunction(allParams);
    }

    /**
     * Overridden by subclasses to provide the list of table function parameters for
     * this specific input format. This list is combined with parameters for input
     * formats. The method is called only once per run.
     */
    protected abstract List<ParameterDefn> fullTableFnParameters();

    /**
     * Add format properties to the base set, in the order of the formats,
     * in the order defined by the format. Allow same-named properties across
     * formats, as long as the types are the same.
     */
    private List<ParameterDefn> addFormatParameters(
        final List<ParameterDefn> properties
    )
    {
      final List<ParameterDefn> toAdd = new ArrayList<>();
      final ParameterDefn formatProp = new Parameter(FORMAT_PROPERTY, String.class, false);
      toAdd.add(formatProp);
      final Map<String, ParameterDefn> formatProps = new HashMap<>();
      for (InputFormatDefn format : formats.values()) {
        for (ParameterDefn prop : format.parameters()) {
          final ParameterDefn existing = formatProps.putIfAbsent(prop.name(), prop);
          if (existing == null) {
            toAdd.add(prop);
          } else if (existing.type() != prop.type()) {
            throw new ISE(
                "Format %s, property %s of class %s conflicts with another format property of class %s",
                format.typeValue(),
                prop.name(),
                prop.type().getSimpleName(),
                existing.type().getSimpleName()
            );
          }
        }
      }
      return CatalogUtils.concatLists(properties, toAdd);
    }

    /**
     * Given a an input format as a generic Java map, find the input format type and
     * resolve that to the corresponding input format definition.
     */
    protected InputFormatDefn formatDefn(Map<String, Object> args)
    {
      final String formatTag = CatalogUtils.getString(args, FORMAT_PROPERTY);
      if (formatTag == null) {
        throw new IAE("%s property must be set", FORMAT_PROPERTY);
      }
      return formatDefn(formatTag);
    }

    /**
     * Given a an input format type string,
     * resolve that to the corresponding input format definition.
     */
    protected InputFormatDefn formatDefn(final String formatTag)
    {
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
    protected InputFormat convertAdHocFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      return formatDefn(args).fromFnArgs(args, columns, jsonMapper);
    }

    @Override
    protected InputFormat convertFormat(ResolvedExternalTable table)
    {
      final String formatKey = CatalogUtils.getString(table.formatMap(), InputFormat.TYPE_PROPERTY);
      return formatDefn(formatKey).fromTable(table);
    }
  }

  /**
   * Describes an inline input source: one where the data is provided in the
   * table spec as a series of text lines. Since the data is provided, the input
   * format is required in the table spec: it cannot be provided at ingest time.
   * Primarily for testing.
   */
  public static class InlineInputSourceDefn extends FormattedInputSourceDefn
  {
    public static final String TYPE_KEY = InlineInputSource.TYPE_KEY;
    public static final String DATA_PROPERTY = "data";

    public class PartialTableFunction extends BaseFunctionDefn
    {
      private final ResolvedExternalTable table;

      public PartialTableFunction(final ResolvedExternalTable table)
      {
        super(Collections.emptyList());
        this.table = table;
      }

      @Override
      public ExternalTableSpec apply(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
      {
        if (!args.isEmpty()) {
          throw new ISE("Cannot provide arguments for an inline table");
        }
        if (!columns.isEmpty()) {
          throw new ISE("Cannot provide columns for an inline table");
        }
        return convertTable(table);
      }
    }

    @Override
    protected List<ParameterDefn> fullTableFnParameters()
    {
      return Collections.singletonList(
          new Parameter(DATA_PROPERTY, String.class, false)
      );
    }

    @Override
    public String typeValue()
    {
      return TYPE_KEY;
    }

    @Override
    public TableFunction tableFn(ResolvedExternalTable table)
    {
      return new PartialTableFunction(table);
    }

    @Override
    protected Class<? extends InputSource> inputSourceClass()
    {
      return InlineInputSource.class;
    }

    @Override
    public void validate(ResolvedExternalTable table)
    {
      // For inline, format is required to match the data
      if (table.formatMap() == null) {
        throw new IAE("An inline input source must provide a format.");
      }
      super.validate(table);
    }

    @Override
    protected void convertAdHocArgs(Map<String, Object> jsonMap, Map<String, Object> args)
    {
      jsonMap.put(InputSource.TYPE_PROPERTY, InlineInputSource.TYPE_KEY);
      String data = CatalogUtils.getString(args, DATA_PROPERTY);

      // Would be nice, from a completeness perspective, for the inline data
      // source to allow zero rows of data. However, such is not the case.
      if (Strings.isNullOrEmpty(data)) {
        throw new IAE(
            "An inline table requires one or more rows of data in the '%s' property",
            DATA_PROPERTY
        );
      }
      jsonMap.put("data", data);
    }

    @Override
    protected void auditTableProperties(Map<String, Object> jsonMap)
    {
      // Special handling of the data property which, in SQL, is a null-delimited
      // list of rows. The user will usually provide a trailing newline which should
      // not be interpreted as an empty data row. That is, if the data ends with
      // a newline, the inline input source will interpret that as a blank line, oddly.
      String data = CatalogUtils.getString(jsonMap, "data");
       if (data != null && data.endsWith("\n")) {
        jsonMap.put("data", data.trim());
      }
    }
  }
}
