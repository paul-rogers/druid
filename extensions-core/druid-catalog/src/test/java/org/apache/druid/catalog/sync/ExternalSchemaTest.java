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

package org.apache.druid.catalog.sync;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Table;
import org.apache.druid.catalog.CatalogException;
import org.apache.druid.catalog.ExternalTableBuilder;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableId;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.TableSpec;
import org.apache.druid.catalog.model.table.ExternalTableDefn.FormattedExternalTableDefn;
import org.apache.druid.catalog.model.table.InlineTableDefn;
import org.apache.druid.catalog.model.table.InputFormats;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.storage.sql.ExternalSchema;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.sql.calcite.table.ExternalTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Mini integration test of the input schema. Creates an input schema
 * directly and via a cached catalog, to ensure changes are propagated.
 */
public class ExternalSchemaTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule =
      new TestDerbyConnector.DerbyConnectorRule();

  private ObjectMapper jsonMapper = new ObjectMapper();
  private TableDefnRegistry registry = new TableDefnRegistry(jsonMapper);
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

  private ExternalSchema inputSchema(MetadataCatalog catalog)
  {
    return new ExternalSchema(
        catalog,
        CatalogTests.JSON_MAPPER
    );
  }

  @Test
  public void testDirect() throws CatalogException
  {
    populateCatalog();
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry());
    ExternalSchema schema = inputSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  @Test
  public void testCached() throws CatalogException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(
        storage,
        storage.schemaRegistry(),
        jsonMapper
    );
    storage.register(catalog);
    ExternalSchema schema = inputSchema(catalog);
    verifyInitial(schema);
    alterCatalog();
    verifyAltered(schema);
  }

  private void populateCatalog() throws CatalogException
  {
    TableSpec inputSpec = new ExternalTableBuilder(registry.defnFor(InlineTableDefn.TABLE_TYPE))
        .property(FormattedExternalTableDefn.FORMAT_PROPERTY, InputFormats.CSV_FORMAT_TYPE)
        .property(InlineTableDefn.DATA_PROPERTY, Arrays.asList("a\n", "c\n"))
        .column("a", "varchar")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.external("input1"),
        inputSpec
    );
    storage.tables().create(table);
  }

  private void verifyInitial(ExternalSchema schema)
  {
    assertNull(schema.getTable("input2"));

    Table table = schema.getTable("input1");
    assertTrue(table instanceof ExternalTable);
    assertEquals(1, ((ExternalTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(1, names.size());
    assertTrue(names.contains("input1"));
  }

  private void alterCatalog() throws CatalogException
  {
    // Add a column to table 1
    TableId id1 = TableId.external("input1");
    TableMetadata table1 = storage.tables().read(id1);
    assertNotNull(table1);
    ResolvedTable resolved1 = registry.resolve(table1.spec());

    TableSpec defn = ExternalTableBuilder.from(resolved1)
        .column("b", "DOUBLE")
        .build();
    storage.tables().update(TableMetadata.of(id1, defn), table1.updateTime());

    // Create a table 2
    TableSpec inputSpec = new ExternalTableBuilder(registry.defnFor(InlineTableDefn.TABLE_TYPE))
        .property(FormattedExternalTableDefn.FORMAT_PROPERTY, InputFormats.CSV_FORMAT_TYPE)
        .property(InlineTableDefn.DATA_PROPERTY, Arrays.asList("1\n", "2\n"))
        .column("x", "bigint")
        .build();
    TableMetadata table = TableMetadata.newTable(
        TableId.external("input2"),
        inputSpec
    );
    storage.tables().create(table);
  }

  private void verifyAltered(ExternalSchema schema)
  {
    Table table = schema.getTable("input1");
    assertTrue(table instanceof ExternalTable);
    assertEquals(2, ((ExternalTable) table).getRowSignature().size());

    table = schema.getTable("input2");
    assertTrue(table instanceof ExternalTable);
    assertEquals(1, ((ExternalTable) table).getRowSignature().size());

    Set<String> names = schema.getTableNames();
    assertEquals(2, names.size());
    assertTrue(names.contains("input1"));
    assertTrue(names.contains("input2"));
  }
}