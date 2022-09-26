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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverterImpl;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InputFormatTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testCsvFormat()
  {
    JsonSubclassConverter<CsvInputFormat> converter = InputFormats.CSV_FORMAT_CONVERTER;
    JsonSubclassConverterImpl<CsvInputFormat> converterImpl = (JsonSubclassConverterImpl<CsvInputFormat>) converter;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // Basic case
    {
      Map<String, Object> args = ImmutableMap.of(
          "listDelimiter", "|", "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      Map<String, Object> jsonMap = converterImpl.gatherFields(table);
      Map<String, Object> expected = ImmutableMap.of(
          "listDelimiter",
          "|",
          "skipHeaderRows",
          1,
          "hasHeaderRow",
          false,
          "columns",
          Arrays.asList("x", "y")
      );
      assertEquals(expected, jsonMap);

      CsvInputFormat expectedFormat = new CsvInputFormat(
          Arrays.asList("x", "y"),
          "|",
          false,
          false,
          1
      );
      CsvInputFormat inputFormat = converter.convert(table, "type");
      assertEquals(expectedFormat, inputFormat);
    }

    // Minimal case. (However, though skipRows is required, JSON will handle
    // a null value and set the value to 0.)
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      CsvInputFormat expectedFormat = new CsvInputFormat(
          Arrays.asList("x", "y"),
          null,
          false,
          false,
          1
      );
      CsvInputFormat inputFormat = converter.convert(table, "type");
      assertEquals(expectedFormat, inputFormat);
    }

    // Invalid format
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", "bogus"
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table, "type"));
    }

    // No columns
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table, "type"));
    }
  }

  @Test
  public void testFlatTextFormat()
  {
    JsonSubclassConverter<DelimitedInputFormat> converter = InputFormats.FLAT_TEXT_FORMAT_CONVERTER;
    JsonSubclassConverterImpl<DelimitedInputFormat> converterImpl = (JsonSubclassConverterImpl<DelimitedInputFormat>) converter;

    Map<String, Object> args = ImmutableMap.of(
        "delimiter", ",", "listDelimiter", "|", "skipRows", 1
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    Map<String, Object> jsonMap = converterImpl.gatherFields(table);
    Map<String, Object> expected = ImmutableMap.of(
        "delimiter",
        ",",
        "listDelimiter",
        "|",
        "skipHeaderRows",
        1,
        "hasHeaderRow",
        false,
        "columns",
        Arrays.asList("x", "y")
    );
    assertEquals(expected, jsonMap);

    DelimitedInputFormat expectedFormat = new DelimitedInputFormat(
        Arrays.asList("x", "y"),
        "|",
        ",",
        false,
        false,
        1
    );
    DelimitedInputFormat inputFormat = converter.convert(table, "type");
    assertEquals(expectedFormat, inputFormat);
  }

  @Test
  public void testJsonFormat()
  {
    JsonSubclassConverter<JsonInputFormat> converter = InputFormats.JSON_FORMAT_CONVERTER;
    JsonSubclassConverterImpl<JsonInputFormat> converterImpl = (JsonSubclassConverterImpl<JsonInputFormat>) converter;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // The one supported property at present.
    {
      Map<String, Object> args = ImmutableMap.of(
          "keepNulls", true
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      JsonInputFormat inputFormat = converter.convert(table, "type");
      assertEquals(new JsonInputFormat(null, null, true), inputFormat);
    }

    // Empty
    {
      TableSpec spec = new TableSpec("type", null, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      Map<String, Object> jsonMap = converterImpl.gatherFields(table);
      assertTrue(jsonMap.isEmpty());

      JsonInputFormat inputFormat = converter.convert(table, "type");
      assertEquals(new JsonInputFormat(null, null, null), inputFormat);
    }
  }
}
