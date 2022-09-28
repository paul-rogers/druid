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

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.table.ExternalSpec;
import org.apache.druid.catalog.specs.table.HttpTableDefn;
import org.apache.druid.catalog.specs.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn;
import org.apache.druid.catalog.specs.table.InputTableDefn.FormattedInputTableDefn;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

public class HttpInputTableTest
{
  private final ObjectMapper mapper = new ObjectMapper();
  private final List<ColumnSpec> cols = Arrays.asList(
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "x", Columns.VARCHAR, null),
      new ColumnSpec(InputTableDefn.INPUT_COLUMN_TYPE, "y", Columns.BIGINT, null)
  );
  HttpTableDefn tableDefn = new HttpTableDefn();

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
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.USER_PROPERTY, "bob")
        .put(HttpTableDefn.PASSWORD_PROPERTY, "secret")
        .put(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("http://foo.com/my.csv"))
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Check serialization
    byte[] bytes = table.spec().toBytes(mapper);
    assertEquals(spec, TableSpec.fromBytes(mapper, bytes));

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource();
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("secret", ((DefaultPasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getPassword());
    assertEquals("http://foo.com/my.csv", sourceSpec.getUris().get(0).toString());

    // Just a sanity check: details of CSV conversion are tested elsewhere.
    CsvInputFormat csvFormat = (CsvInputFormat) externSpec.inputFormat();
    assertEquals(Arrays.asList("x", "y"), csvFormat.getColumns());

    RowSignature sig = externSpec.signature();
    assertEquals(Arrays.asList("x", "y"), sig.getColumnNames());
    assertEquals(ColumnType.STRING, sig.getColumnType(0).get());
    assertEquals(ColumnType.LONG, sig.getColumnType(1).get());
  }

  @Test
  public void testEnvPassword()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.USER_PROPERTY, "bob")
        .put(HttpTableDefn.PASSWORD_ENV_VAR_PROPERTY, "SECRET")
        .put(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("http://foo.com/my.csv"))
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Convert to an external spec
    ExternalSpec externSpec = tableDefn.convertToExtern(table);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource();
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("SECRET", ((EnvironmentVariablePasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getVariable());
  }

  @Test
  public void testParameters()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.USER_PROPERTY, "bob")
        .put(HttpTableDefn.PASSWORD_ENV_VAR_PROPERTY, "SECRET")
        .put(HttpTableDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Parameters
    Parameterized parameterizedTable = tableDefn;
    assertEquals(1, parameterizedTable.parameters().size());
    assertNotNull(parameterizedTable.parameters().get(HttpTableDefn.URIS_PARAMETER));

    // Apply parameters
    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PARAMETER, "foo.csv,bar.csv"
    );

    // Convert to an external spec
    ExternalSpec externSpec = parameterizedTable.applyParameters(table, params);

    HttpInputSource sourceSpec = (HttpInputSource) externSpec.inputSource();
    assertEquals("bob", sourceSpec.getHttpAuthenticationUsername());
    assertEquals("SECRET", ((EnvironmentVariablePasswordProvider) sourceSpec.getHttpAuthenticationPasswordProvider()).getVariable());
    assertEquals(
        HttpTableDefn.convertUriList(Arrays.asList("http://foo.com/foo.csv", "http://foo.com/bar.csv")),
        sourceSpec.getUris()
    );
  }

  @Test
  public void testNoTemplate()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    // Check validation
    table.validate();

    // Apply parameters
    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PARAMETER, "foo.csv,bar.csv"
    );

    // Convert to an external spec
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testNoParameters()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.URI_TEMPLATE_PROPERTY, "http://foo.com/{}")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    Map<String, Object> params = ImmutableMap.of();
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testInvalidParameters()
  {
    // The URI parser is forgiving about items in the path, so
    // screw up the head, where URI is particular.
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.URI_TEMPLATE_PROPERTY, "//foo.com/{}")
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    Map<String, Object> params = ImmutableMap.of(
        HttpTableDefn.URIS_PARAMETER, "foo.csv"
    );
    assertThrows(IAE.class, () -> tableDefn.applyParameters(table, params));
  }

  @Test
  public void testInvalidURI()
  {
    // The URI parser is forgiving about items in the path, so
    // screw up the head, where URI is particular.
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(TableDefn.DESCRIPTION_PROPERTY, "http input")
        .put(HttpTableDefn.URIS_PROPERTY, Collections.singletonList("//foo.com"))
        .put(FormattedInputTableDefn.INPUT_FORMAT_PROPERTY, CsvFormatDefn.FORMAT_KEY)
        .build();
    TableSpec spec = new TableSpec(HttpTableDefn.TABLE_TYPE, props, cols);
    ResolvedTable table = new ResolvedTable(tableDefn, spec, mapper);

    assertThrows(IAE.class, () -> table.validate());
  }
}
