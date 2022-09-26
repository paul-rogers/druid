package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the combination of the individual input formats and sources.
 * The formats and sources are tested in detail elsewhere.
 */
public class ExternalSpecTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testInputFormat()
  {
    JsonUnionConverter<InputFormat> converter = InputFormats.INPUT_FORMAT_CONVERTER;
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec("type", "x", Columns.VARCHAR, null),
        new ColumnSpec("type", "y", Columns.BIGINT, null)
    );

    // CSV
    {
      Map<String, Object> args = ImmutableMap.of(
          InputFormats.INPUT_FORMAT_PROPERTY, CsvInputFormat.TYPE_KEY,
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);
      assertTrue(converter.convert(table) instanceof CsvInputFormat);
    }

    // Delimited text
    {
      Map<String, Object> args = ImmutableMap.of(
          InputFormats.INPUT_FORMAT_PROPERTY, DelimitedInputFormat.TYPE_KEY,
          "delimiter", ",",
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);
      assertTrue(converter.convert(table) instanceof DelimitedInputFormat);
    }

    // JSON
    {
      Map<String, Object> args = ImmutableMap.of(
          InputFormats.INPUT_FORMAT_PROPERTY, JsonInputFormat.TYPE_KEY,
          "keepNulls", true
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);
      assertTrue(converter.convert(table) instanceof JsonInputFormat);
    }

    // Invalid
    {
      Map<String, Object> args = ImmutableMap.of(
          InputFormats.INPUT_FORMAT_PROPERTY, "bogus",
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);
      assertThrows(IAE.class, () -> converter.convert(table));
    }

    // No type
    {
      Map<String, Object> args = ImmutableMap.of(
          "skipRows", 1
      );
      TableSpec spec = new TableSpec("type", args, cols);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);
      assertThrows(IAE.class, () -> converter.convert(table));
    }
  }

  public void testInputSource()
  {

  }

  public void testExternalSpec()
  {

  }
}
