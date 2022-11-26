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
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.table.BaseFunctionDefn.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class InputFormats
{
  /**
   * Base class for input format definitions.
   */
  public abstract static class BaseFormatDefn implements InputFormatDefn
  {
    /**
     * The set of SQL function parameters available when the format is
     * specified via a SQL function. The parameters correspond to input format
     * properties, but typically have simpler names and must require only simple
     * scalar types of the kind that SQL can provide. Each subclass must perform
     * conversion to the type required for Jackson conversion.
     */
    private final List<ParameterDefn> parameters;

    public BaseFormatDefn(List<ParameterDefn> parameters)
    {
      this.parameters = parameters;
    }

    @Override
    public List<ParameterDefn> parameters()
    {
      return parameters;
    }

    /**
     * The target input format class for Jackson conversions.
     */
    protected abstract Class<? extends InputFormat> inputFormatClass();

    /**
     * Convert columns from the {@link ColumnSpec} format used by the catalog to the
     * list of names form requires by input formats.
     */
    protected void convertColumns(Map<String, Object> jsonMap, List<ColumnSpec> columns)
    {
      List<String> cols = columns
          .stream()
          .map(col -> col.name())
          .collect(Collectors.toList());
      jsonMap.put("columns", cols);
    }

    /**
     * Convert a generic Java map of input format properties to an input format object.
     */
    public InputFormat convert(
        final Map<String, Object> jsonMap,
        final ObjectMapper jsonMapper
    )
    {
      try {
        return jsonMapper.convertValue(jsonMap, inputFormatClass());
      }
      catch (Exception e) {
        throw new IAE(e, "Invalid format specification");
      }
    }
  }

  /**
   * Definition of a flat text (CSV and delimited text) input format.
   * <p>
   * Note that not all the fields in
   * {@link org.apache.druid.data.input.impl.FlatTextInputFormat
   * FlatTextInputFormat} appear here:
   * <ul>
   * <li>{@code findColumnsFromHeader} - not yet supported in MSQ.</li>
   * <li>{@code hasHeaderRow} - Always set to false since we don't bother to read
   * it. {@code skipHeaderRows} is used to specify the number of header
   * rows to skip.</li>
   * </ul>
   */
  public abstract static class FlatTextFormatDefn extends BaseFormatDefn
  {
    public static final String LIST_DELIMITER_PARAM = "listDelimiter";
    public static final String SKIP_ROWS_PARAM = "skipRows";

    public FlatTextFormatDefn(List<ParameterDefn> parameters)
    {
      super(
          CatalogUtils.concatLists(
              Arrays.asList(
                  new Parameter(LIST_DELIMITER_PARAM, String.class, true),
                  new Parameter(SKIP_ROWS_PARAM, Boolean.class, true)
              ),
              parameters
          )
      );
    }

    @Override
    public void validate(ResolvedExternalTable table)
    {
      if (table.formatMap() == null) {
        return;
      }
      ResolvedTable resolvedTable = table.resolvedTable();
      Map<String, Object> jsonMap = toMap(table);
      if (!jsonMap.containsKey("columns")) {
        // Make up a column just so that validation will pass.
        jsonMap.put("columns", Collections.singletonList("a"));
      }
      convert(jsonMap, resolvedTable.jsonMapper());
    }

    protected Map<String, Object> toMap(ResolvedExternalTable table)
    {
      ResolvedTable resolvedTable = table.resolvedTable();
      Map<String, Object> jsonMap = new HashMap<>(table.formatMap());
      if (!CollectionUtils.isNullOrEmpty(resolvedTable.spec().columns())) {
        convertColumns(jsonMap, resolvedTable.spec().columns());
      }
      adjustValues(jsonMap);
      return jsonMap;
    }

    protected void adjustValues(Map<String, Object> jsonMap)
    {
      // findColumnsFromHeader is required, even though we don't infer headers.
      jsonMap.put("findColumnsFromHeader", false);
      jsonMap.computeIfAbsent("skipHeaderRows", key -> 0);
    }

    protected Map<String, Object> mapFromArgs(Map<String, Object> args, List<ColumnSpec> columns)
    {
      Map<String, Object> jsonMap = new HashMap<>();
      jsonMap.put("listDelimiter", args.get(LIST_DELIMITER_PARAM));
      Object value = args.get(SKIP_ROWS_PARAM);
      jsonMap.put("skipHeaderRows", value == null ? 0 : value);
      convertColumns(jsonMap, columns);
      adjustValues(jsonMap);
      return jsonMap;
    }
  }

  /**
   * Definition for the CSV input format. Designed so that, in most cases, the
   * user only need specify the format as CSV: the definition fills in the common
   * "boiler plate" properties.
   */
  public static class CsvFormatDefn extends FlatTextFormatDefn
  {
    public static final String TYPE_KEY = CsvInputFormat.TYPE_KEY;

    public CsvFormatDefn()
    {
      super(null);
    }

    @Override
    public String typeValue()
    {
      return TYPE_KEY;
    }

    @Override
    protected Class<? extends InputFormat> inputFormatClass()
    {
       return CsvInputFormat.class;
    }

    @Override
    public InputFormat fromFnArgs(
        Map<String, Object> args,
        List<ColumnSpec> columns,
        ObjectMapper jsonMapper
    )
    {
      Map<String, Object> jsonMap = mapFromArgs(args, columns);
      jsonMap.put(InputFormat.TYPE_PROPERTY, CsvInputFormat.TYPE_KEY);
      return convert(jsonMap, jsonMapper);
    }

    @Override
    public InputFormat fromTable(ResolvedExternalTable table)
    {
      return convert(toMap(table), table.resolvedTable().jsonMapper());
    }
  }
}
