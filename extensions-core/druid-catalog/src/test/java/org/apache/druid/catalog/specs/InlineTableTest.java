package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.table.ExternalSpec;
import org.apache.druid.catalog.specs.table.InlineTableDefn;
import org.apache.druid.catalog.specs.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn.FormattedInputTableDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InlineTableTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<ColumnSpec> cols = Arrays.asList(
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "x", Columns.VARCHAR, null),
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "y", Columns.BIGINT, null)
  );
  InlineTableDefn tableDefn = new InlineTableDefn();

  @Test
  public void testEmptyData()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "inline input")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(InlineTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    assertThrows(IAE.class, () -> table.validate());
  }

  @Test
  public void testValidData()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "inline input")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .put(InlineTableDefn.DATA_PROPERTY, Arrays.asList("a,b", "c,d"))
        .build();
    TableSpec spec = new TableSpec(InlineTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Check serialization
    byte[] bytes = table.spec().toBytes(mapper);
    assertEquals(spec, TableSpec.fromBytes(mapper, bytes));

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    InlineInputSource inlineSpec = (InlineInputSource) externSpec.inputSource();
    assertEquals("a,b\nc,d\n", inlineSpec.getData());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat();
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature();
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }
}
