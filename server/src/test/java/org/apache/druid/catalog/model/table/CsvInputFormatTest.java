package org.apache.druid.catalog.model.table;

import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class CsvInputFormatTest extends BaseExternTableTest
{
  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testDefaults()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a\n"))
        .inputFormat("{\"type\": \"" + CsvInputFormat.TYPE_KEY + "\"}")
        .column("a", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    InputFormatDefn defn = registry.inputFormatDefnFor(CsvInputFormat.TYPE_KEY);
    InputFormat inputFormat = defn.convertFromTable(new ResolvedExternalTable(resolved));
    CsvInputFormat csvFormat = (CsvInputFormat) inputFormat;
    assertEquals(0, csvFormat.getSkipHeaderRows());
    assertFalse(csvFormat.isFindColumnsFromHeader());
    assertNull(csvFormat.getListDelimiter());
    assertEquals(Collections.singletonList("a"), csvFormat.getColumns());
  }

  @Test
  public void testConversion()
  {
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 1);
    Map<String, Object> formatMap = toMap(format);
    formatMap.remove("columns");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a\n"))
        .inputFormat(toJsonString(formatMap))
        .column("a", Columns.VARCHAR)
        .column("b", Columns.BIGINT)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    InputFormatDefn defn = registry.inputFormatDefnFor(CsvInputFormat.TYPE_KEY);
    InputFormat inputFormat = defn.convertFromTable(new ResolvedExternalTable(resolved));
    CsvInputFormat csvFormat = (CsvInputFormat) inputFormat;
    assertEquals(1, csvFormat.getSkipHeaderRows());
    assertFalse(csvFormat.isFindColumnsFromHeader());
    assertEquals(";", csvFormat.getListDelimiter());
    assertEquals(Arrays.asList("a", "b"), csvFormat.getColumns());
  }

  @Test
  public void testFunctionParams()
  {
    InputFormatDefn defn = registry.inputFormatDefnFor(CsvInputFormat.TYPE_KEY);
    List<ParameterDefn> params = defn.parameters();
    assertEquals(2, params.size());
  }

  @Test
  public void testCreateFromArgs()
  {
    Map<String, Object> args = new HashMap<>();
    args.put(CsvFormatDefn.LIST_DELIMITER_PARAM, ";");
    args.put(CsvFormatDefn.SKIP_ROWS_PARAM, 1);
    InputFormatDefn defn = registry.inputFormatDefnFor(CsvInputFormat.TYPE_KEY);
    List<ColumnSpec> columns = Collections.singletonList(new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, "a", null, null));
    InputFormat inputFormat = defn.convertFromArgs(args, columns, mapper);
    CsvInputFormat csvFormat = (CsvInputFormat) inputFormat;
    assertEquals(1, csvFormat.getSkipHeaderRows());
    assertFalse(csvFormat.isFindColumnsFromHeader());
    assertEquals(";", csvFormat.getListDelimiter());
    assertEquals(Collections.singletonList("a"), csvFormat.getColumns());
  }
}
