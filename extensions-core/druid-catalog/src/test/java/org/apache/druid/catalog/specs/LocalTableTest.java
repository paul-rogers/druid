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
import org.apache.druid.catalog.specs.table.ExternalSpec;
import org.apache.druid.catalog.specs.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn.FormattedInputTableDefn;
import org.apache.druid.catalog.specs.table.LocalTableDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class LocalTableTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<ColumnSpec> cols = Arrays.asList(
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "x", Columns.VARCHAR, null),
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "y", Columns.BIGINT, null)
  );
  LocalTableDefn tableDefn = new LocalTableDefn();

  @Test
  public void testFullyDefined()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(LocalTableDefn.FILE_FILTER_PROPERTY, "*.csv")
        .put(LocalTableDefn.FILES_PROPERTY, Collections.singletonList("my.csv"))
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Check serialization
    byte[] bytes = table.spec().toBytes(mapper);
    assertEquals(spec, TableSpec.fromBytes(mapper, bytes));

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource();
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*.csv", sourceSpec.getFilter());
    assertEquals("my.csv", sourceSpec.getFiles().get(0).toString());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat();
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature();
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }

  @Test
  public void testNoFilter()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(LocalTableDefn.FILES_PROPERTY, Collections.singletonList("my.csv"))
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource();
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*", sourceSpec.getFilter());
    assertEquals("my.csv", sourceSpec.getFiles().get(0).toString());
  }

  @Test
  public void testNoFiles()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(LocalTableDefn.FILE_FILTER_PROPERTY, "*.csv")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource();
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*.csv", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
  }

  @Test
  public void testNoFilesOrFlter()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    assertThrows(IAE.class, () -> table.validate());
  }

  @Test
  public void testNoProperties()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
         .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation: is legal for storage, but needs
    // paramters to be valid at runtime.
    table.validate();
  }

  @Test
  public void testFilesParameter()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    Parameterized parameterizedTable = tableDefn;
    assertEquals(2, parameterizedTable.parameters().size());
    assertNotNull(parameterizedTable.parameters().get(LocalTableDefn.FILE_FILTER_PROPERTY));
    assertNotNull(parameterizedTable.parameters().get(LocalTableDefn.FILES_PROPERTY));


    // Apply files parameter
    Map<String, Object> params = ImmutableMap.of(
        LocalTableDefn.FILES_PROPERTY, "foo.csv,bar.csv"
    );

    // Convert to an external spec
    ExternalSpec externSpec = parameterizedTable.applyParameters(table, params);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource();
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("*", sourceSpec.getFilter());
    assertEquals(
        Arrays.asList(new File("foo.csv"), new File("bar.csv")),
        sourceSpec.getFiles()
    );
  }

  @Test
  public void testFilterParameter()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "local file input")
        .put(LocalTableDefn.BASE_DIR_PROPERTY, "/tmp")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(LocalTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    Parameterized parameterizedTable = tableDefn;
    assertEquals(2, parameterizedTable.parameters().size());
    assertNotNull(parameterizedTable.parameters().get(LocalTableDefn.FILE_FILTER_PROPERTY));
    assertNotNull(parameterizedTable.parameters().get(LocalTableDefn.FILES_PROPERTY));


    // Apply files parameter
    Map<String, Object> params = ImmutableMap.of(
        LocalTableDefn.FILE_FILTER_PROPERTY, "Oct*.csv"
    );

    // Convert to an external spec
    ExternalSpec externSpec = parameterizedTable.applyParameters(table, params);

    LocalInputSource sourceSpec = (LocalInputSource) externSpec.inputSource();
    assertEquals("/tmp", sourceSpec.getBaseDir().toString());
    assertEquals("Oct*.csv", sourceSpec.getFilter());
    assertTrue(sourceSpec.getFiles().isEmpty());
  }
}
