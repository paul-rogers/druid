package org.apache.druid.catalog.model.table;

import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.catalog.model.table.InputSources.InlineInputSourceDefn;
import org.apache.druid.catalog.model.table.InputSources.FormattedInputSourceDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.model.table.InputFormats.FlatTextFormatDefn;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class InlineInputSourceTest extends BaseExternTableTest
{
  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testValidateEmptyInputSource()
  {
    // No data property: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + InlineInputSource.TYPE_KEY + "\"}")
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateNoFormat()
  {
    // No format: not valid. For inline, format must be provided to match data
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a\n"))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateGood()
  {
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a\n"))
        .inputFormat(formatToJson(format))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testFullTableFnBasics()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.externFn();
    assertNotNull(fn);
    assertTrue(hasParam(fn, InlineInputSourceDefn.DATA_PROPERTY));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PROPERTY));
    assertTrue(hasParam(fn, FlatTextFormatDefn.LIST_DELIMITER_PARAM));
  }

  @Test
  public void testMissingArgs()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.externFn();
    assertThrows(IAE.class, () -> fn.apply(new HashMap<>(), Collections.emptyList(), mapper));
  }

  @Test
  public void testMissingFormat()
  {
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    TableFunction fn = defn.externFn();
    Map<String, Object> args = new HashMap<>();
    args.put(InlineInputSourceDefn.DATA_PROPERTY, "a");
    assertThrows(IAE.class, () -> fn.apply(new HashMap<>(), Collections.emptyList(), mapper));
  }

  @Test
  public void testValidFullFn()
  {
    // Simulate the information obtained from an SQL table function
    InputSourceDefn defn = registry.inputSourceDefnFor(InlineInputSourceDefn.TYPE_KEY);
    Map<String, Object> args = new HashMap<>();
    args.put(InlineInputSourceDefn.DATA_PROPERTY, "a,b\nc,d");
    args.put(FormattedInputSourceDefn.FORMAT_PROPERTY, CsvFormatDefn.TYPE_KEY);
    List<ColumnSpec> columns = Arrays.asList(
        new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, "a", Columns.VARCHAR, null),
        new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, "b", Columns.VARCHAR, null)
    );

    ExternalTableSpec extern = defn.externFn().apply(args, columns, mapper);

    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat format = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), format.getColumns());
    assertEquals(2, extern.signature.size());
  }

  @Test
  public void testPartialTable()
  {
    // Define an inline table
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a,b\nc,d"))
        .inputFormat(formatToJson(format))
        .column("a", Columns.VARCHAR)
        .column("b", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    // Get the partial table function
    TableFunction fn = ((ExternalTableDefn) resolved.defn()).tableFn(resolved);

    // Inline is always fully defined: no arguments needed
    assertTrue(fn.parameters().isEmpty());

    // Verify the conversion
    ExternalTableSpec extern = fn.apply(new HashMap<>(), Collections.emptyList(), mapper);

    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat actualFormat = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), actualFormat.getColumns());
    assertEquals(2, extern.signature.size());
  }

  @Test
  public void testDefinedTable()
  {
    // Define an inline table
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, new InlineInputSource("a,b\nc,d"))
        .inputFormat(formatToJson(format))
        .column("a", Columns.VARCHAR)
        .column("b", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();

    // Inline is always fully defined: can directly convert to a table
    ExternalTableSpec extern = ((ExternalTableDefn) resolved.defn()).convert(resolved);

    // Verify the conversion
    assertTrue(extern.inputSource instanceof InlineInputSource);
    InlineInputSource inputSource = (InlineInputSource) extern.inputSource;
    assertEquals("a,b\nc,d\n", inputSource.getData());
    assertTrue(extern.inputFormat instanceof CsvInputFormat);
    CsvInputFormat actualFormat = (CsvInputFormat) extern.inputFormat;
    assertEquals(Arrays.asList("a", "b"), actualFormat.getColumns());
    assertEquals(2, extern.signature.size());
  }
}
