package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.table.ExternalSpec;
import org.apache.druid.catalog.specs.table.HttpTableDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class HttpInputTableTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<ColumnSpec> cols = Arrays.asList(
      new ColumnSpec("type", "x", Columns.VARCHAR, null),
      new ColumnSpec("type", "y", Columns.BIGINT, null)
  );

  public HttpInputTableTest()
  {
    mapper.setInjectableValues(new InjectableValues.Std().addValue(
        HttpInputSourceConfig.class,
        new HttpInputSourceConfig(HttpInputSourceConfig.DEFAULT_ALLOWED_PROTOCOLS)
    ));
  }

  @Test
  public void testHappyPath()
  {
    HttpTableDefn tableDefn = new HttpTableDefn();
    Map<String, Object> props = ImmutableMap.of(
        "description", "http input",
        "source",
        ImmutableMap.<String, Object>of(
            "type", HttpInputSource.TYPE_KEY,
            "uris", Collections.singletonList("http://foo.com/my.csv"),
            "httpAuthenticationUsername", "bob",
            "httpAuthenticationPassword",
            ImmutableMap.of(
                "type", "default",
                "password", "secret"
            )
        ),
        "format",
        ImmutableMap.<String, Object>of(
            "type", CsvInputFormat.TYPE_KEY
        )
    );
    TableSpec spec = new TableSpec(HttpTableDefn.HTTP_TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Check serialization
    byte[] bytes = table.spec().toBytes(mapper);
    assertEquals(spec, TableSpec.fromBytes(mapper, bytes));

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(spec, mapper);

    HttpInputSource httpSpec = (HttpInputSource) externSpec.inputSource();
    assertEquals("bob", httpSpec.getHttpAuthenticationUsername());
    assertEquals("secret", ((DefaultPasswordProvider) httpSpec.getHttpAuthenticationPasswordProvider()).getPassword());
    assertEquals("http://foo.com/my.csv", httpSpec.getUris().get(0).toString());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat();
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature();
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }

  @Test
  public void testParameters()
  {

  }
}
