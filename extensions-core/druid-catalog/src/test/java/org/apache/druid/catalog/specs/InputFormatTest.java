package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.ColumnSchema;
import org.apache.druid.catalog.model.ExternalSpec;
import org.apache.druid.catalog.model.SubTypeConverter;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
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
    Map<String, Object> args = ImmutableMap.of(
        "listDelimiter", "|", "skipRows", 1
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    Map<String, Object> jsonMap = converter.gatherFields(table);
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

  @Test
  public void testMinimalCsvFormat()
  {
    JsonSubclassConverter<CsvInputFormat> converter = InputFormats.CSV_FORMAT_CONVERTER;
    Map<String, Object> args = ImmutableMap.of(
        "skipRows", 1
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
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

  @Test
  public void testInvalidCsvFormat()
  {
    JsonSubclassConverter<CsvInputFormat> converter = InputFormats.CSV_FORMAT_CONVERTER;
    Map<String, Object> args = ImmutableMap.of(
        "skipRows", "bogus"
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    assertThrows(Exception.class, () -> converter.convert(table, "type"));
  }

  @Test
  public void testFlatTextFormat()
  {
    JsonSubclassConverter<DelimitedInputFormat> converter = InputFormats.FLAT_TEXT_FORMAT_CONVERTER;
    Map<String, Object> args = ImmutableMap.of(
        "delimiter", ",", "listDelimiter", "|", "skipRows", 1
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    Map<String, Object> jsonMap = converter.gatherFields(table);
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

    Map<String, Object> args = ImmutableMap.of(
        "keepNulls", true
    );
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", args, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    JsonInputFormat inputFormat = converter.convert(table, "type");
    assertEquals(new JsonInputFormat(null, null, true), inputFormat);
  }

  @Test
  public void testEmptyJsonFormat()
  {
    JsonSubclassConverter<JsonInputFormat> converter = InputFormats.JSON_FORMAT_CONVERTER;

    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );
    TableSpec spec = new TableSpec("type", null, cols);
    ResolvedTable table = new ResolvedTable(null, spec, mapper);

    Map<String, Object> jsonMap = converter.gatherFields(table);
    assertTrue(jsonMap.isEmpty());

    JsonInputFormat inputFormat = converter.convert(table, "type");
    assertEquals(new JsonInputFormat(null, null, null), inputFormat);
  }
}
