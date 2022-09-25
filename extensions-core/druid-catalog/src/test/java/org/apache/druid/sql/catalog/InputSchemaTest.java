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

package org.apache.druid.sql.catalog;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.Function;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.TableMacro;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.curator.shaded.com.google.common.collect.Iterators;
import org.apache.druid.catalog.CachedMetadataCatalog;
import org.apache.druid.catalog.CatalogStorage;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.CatalogTests;
import org.apache.druid.catalog.InputTableSpec;
import org.apache.druid.catalog.MetadataCatalog;
import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.TableMetadata;
import org.apache.druid.catalog.sync.LocalMetadataCatalog;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.catalog.CatalogManager.DuplicateKeyException;
import org.apache.druid.metadata.catalog.CatalogManager.NotFoundException;
import org.apache.druid.metadata.catalog.CatalogManager.OutOfDateException;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.external.InputTableMacro;
import org.apache.druid.sql.calcite.table.DatasourceTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Mini integration test of the input schema. Creates an input schema
 * directly and via a cached catalog, to ensure changes are propagated.
 */
@Category(CatalogTest.class)
public class InputSchemaTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private InputSchema inputSchema(MetadataCatalog catalog)
  {
    return new InputSchema(
        catalog,
        CatalogTests.JSON_MAPPER,
        new InputTableMacro(CatalogTests.JSON_MAPPER, dbFixture.externModel)
    );
  }

  @Test
  public void testDirect() throws DuplicateKeyException, OutOfDateException, NotFoundException
  {
    populateCatalog();
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry());
    InputSchema schema = inputSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  @Test
  public void testCached() throws DuplicateKeyException, OutOfDateException, NotFoundException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry());
    storage.register(catalog);
    InputSchema schema = inputSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  private void populateCatalog() throws DuplicateKeyException
  {
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a\nc\n"
    );
    InputTableSpec inputSpec = InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.INPUT_SCHEMA,
        "input1",
        inputSpec
    );
    storage.tables().create(table);
  }

  private void verifyInitial(InputSchema schema)
  {
    assertNull(schema.getTable("input2"));

    Table table = schema.getTable("input1");
    assertTrue(table instanceof DatasourceTable);
    assertEquals(1, ((DatasourceTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(1, names.size());
    assertTrue(names.contains("input1"));
  }

  private void alterCatalog() throws DuplicateKeyException, OutOfDateException, NotFoundException
  {
    // Add a column to table 1
    TableId id1 = TableId.inputSource("input1");
    TableMetadata table1 = storage.tables().read(id1);
    assertNotNull(table1);

    InputTableSpec spec = (InputTableSpec) table1.spec();
    spec = spec.toBuilder()
        .column("b", "DOUBLE")
        .build();
    storage.tables().update(TableMetadata.newTable(id1, spec), table1.updateTime());

    // Create a table 2
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "1\n2\n"
    );
    InputTableSpec inputSpec = InputTableSpec
        .builder()
        .properties(props)
        .column("x", "bigint")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.INPUT_SCHEMA,
        "input2",
        inputSpec
    );
    storage.tables().create(table);
  }

  private void verifyAltered(InputSchema schema)
  {
    Table table = schema.getTable("input1");
    assertTrue(table instanceof DatasourceTable);
    assertEquals(2, ((DatasourceTable) table).getRowSignature().size());

    table = schema.getTable("input2");
    assertTrue(table instanceof DatasourceTable);
    assertEquals(1, ((DatasourceTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(2, names.size());
    assertTrue(names.contains("input1"));
    assertTrue(names.contains("input2"));
  }

  @Test
  public void testParameterizedFn() throws DuplicateKeyException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry());
    storage.register(catalog);
    InputSchema schema = inputSchema(catalog);
    CalciteSchema calciteSchema = schema.createSchema(null, schema.getSchemaName());
    Collection<Function> fns = calciteSchema.getFunctions("input1", true);
    assertEquals(1, fns.size());
    Function fn = Iterators.getOnlyElement(fns.iterator());
    TableMacro macro = (TableMacro) fn;

    // No arguments. This is not a full check of the external data source
    // conversion; just a sanity check that the values are passed along
    // as expected.
    {
      TranslatableTable table = macro.apply(Collections.emptyList());
      DatasourceTable druidTable = (DatasourceTable) table;
      assertEquals(1, druidTable.getRowSignature().size());
      ExternalDataSource extds = (ExternalDataSource) druidTable.getDataSource();
      assertTrue(extds.getInputFormat() instanceof CsvInputFormat);
      assertEquals(ImmutableList.of("a"), ((CsvInputFormat) extds.getInputFormat()).getColumns());
      assertTrue(extds.getInputSource() instanceof InlineInputSource);
      assertEquals("a\nc\n", ((InlineInputSource) extds.getInputSource()).getData());
    }

    // Override properties
    {
      Map<String, Object> props = ImmutableMap.of("data", "1\n2\n");
      List<Object> args = dbFixture.externModel.toCalciteArgs(props);
      TranslatableTable table = macro.apply(args);
      DatasourceTable druidTable = (DatasourceTable) table;
      assertEquals(1, druidTable.getRowSignature().size());
      ExternalDataSource extds = (ExternalDataSource) druidTable.getDataSource();
      assertTrue(extds.getInputFormat() instanceof CsvInputFormat);
      assertEquals(ImmutableList.of("a"), ((CsvInputFormat) extds.getInputFormat()).getColumns());
      assertTrue(extds.getInputSource() instanceof InlineInputSource);
      assertEquals("1\n2\n", ((InlineInputSource) extds.getInputSource()).getData());
    }
  }
}
