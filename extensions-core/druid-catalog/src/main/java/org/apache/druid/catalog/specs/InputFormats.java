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

package org.apache.druid.catalog.specs;

import org.apache.druid.catalog.specs.CatalogFieldDefn.BooleanFieldDefn;
import org.apache.druid.catalog.specs.CatalogFieldDefn.StringFieldDefn;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonProperty;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Definition of the input format conversions which converts from property
 * lists in table specs to subclasses of {@link InputFormat}.
 */
public class InputFormats
{
  /**
   * Input format fields for CSV. Note that not all the fields in
   * {@link CsvInputFormat} appear here:
   * <ul>
   * <li>{@code findColumnsFromHeader} - not yet supported in MSQ.</li>
   * <li>{@code hasHeaderRow} - Always set to false since we don't bother to read
   * it. {@code skipHeaderRows} is used to specify the number of header
   * rows to skip.</li>
   * </ul>
   */
  public static final List<JsonProperty> CSV_FORMAT_FIELDS = Arrays.asList(
        new JsonProperty(
            "listDelimiter",
            new StringFieldDefn("listDelimiter")
        ),
        new JsonProperty(
            "skipHeaderRows",
            new BooleanFieldDefn("skipRows")
        )
  );

  public static final List<JsonProperty> FLAT_TEXT_FORMAT_FIELDS = Arrays.asList(
      new JsonProperty(
          "delimiter",
          new StringFieldDefn("delimiter")
      ),
      new JsonProperty(
          "listDelimiter",
          new StringFieldDefn("listDelimiter")
      ),
      new JsonProperty(
          "skipHeaderRows",
          new BooleanFieldDefn("skipRows")
      )
  );

  public static final List<JsonProperty> JSON_FORMAT_FIELDS = Arrays.asList(
      new JsonProperty(
          "keepNullColumns",
          new BooleanFieldDefn("keepNulls")
      )
  );

  public static class FlatTextFormatConverter<T extends InputFormat> extends JsonSubclassConverter<T>
  {
    public FlatTextFormatConverter(
        final String typePropertyValue,
        final String jsonTypeValue,
        final List<JsonProperty> properties,
        final Class<T> targetClass
    )
    {
      super(typePropertyValue, jsonTypeValue, properties, targetClass);
    }

    @Override
    public Map<String, Object> gatherFields(ResolvedTable table)
    {
      Map<String, Object> jsonMap = super.gatherFields(table);
      // hasHeaderRow is required, even though we don't infer headers.
      jsonMap.put("hasHeaderRow", false);
      // Column list is required. Infer from schema.
      List<String> cols = table.spec().columns()
          .stream()
          .map(col -> col.name())
          .collect(Collectors.toList());
      jsonMap.put("columns", cols);
      return jsonMap;
    }
  }

  public static final JsonSubclassConverter<CsvInputFormat> CSV_FORMAT_CONVERTER =
      new FlatTextFormatConverter<>(
          CsvInputFormat.TYPE_KEY,
          CsvInputFormat.TYPE_KEY,
          CSV_FORMAT_FIELDS,
          CsvInputFormat.class
      );

  public static final JsonSubclassConverter<DelimitedInputFormat> FLAT_TEXT_FORMAT_CONVERTER =
      new FlatTextFormatConverter<>(
          DelimitedInputFormat.TYPE_KEY,
          DelimitedInputFormat.TYPE_KEY,
          FLAT_TEXT_FORMAT_FIELDS,
          DelimitedInputFormat.class
      );

  public static final JsonSubclassConverter<JsonInputFormat> JSON_FORMAT_CONVERTER =
      new JsonSubclassConverter<>(
          JsonInputFormat.TYPE_KEY,
          JsonInputFormat.TYPE_KEY,
          JSON_FORMAT_FIELDS,
          JsonInputFormat.class
      );

  public static final CatalogFieldDefn<String> INPUT_FORMAT_FIELD = new StringFieldDefn("inputFormat");

  /**
   * Converter for the set of input formats as a union with the type determined
   * by (key, value) pairs in both the property and JSON representations.
   */
  public static final JsonUnionConverter<InputFormat> INPUT_FORMAT_CONVERTER =
      new JsonUnionConverter<>(
          InputFormat.class.getSimpleName(),
          INPUT_FORMAT_FIELD.name(),
          "type",
          Arrays.asList(
              CSV_FORMAT_CONVERTER,
              FLAT_TEXT_FORMAT_CONVERTER,
              JSON_FORMAT_CONVERTER
          )
      );
}
