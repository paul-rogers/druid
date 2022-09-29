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

import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.TableBuilder;
import org.apache.druid.catalog.storage.CatalogStorage;
import org.apache.druid.catalog.storage.CatalogTests;
import org.apache.druid.catalog.storage.sql.CatalogManager.DuplicateKeyException;
import org.apache.druid.catalog.sync.LocalMetadataCatalog;
import org.apache.druid.catalog.sync.MetadataCatalog;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.query.TableDataSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.planner.CatalogResolver;
import org.apache.druid.sql.calcite.table.DatasourceTable.PhysicalDatasourceMetadata;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.fail;

/**
 * Test for the datasource resolution aspects of the live catalog resolver.
 * Too tedious to test the insert resolution in its current state.
 */
public class LiveCatalogTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;
  private CatalogResolver resolver;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry());
    resolver = new LiveCatalogResolver(catalog);
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private void createTableMetadata(TableMetadata table)
  {
    try {
      storage.tables().create(table);
    }
    catch (DuplicateKeyException e) {
      fail(e.getMessage());
    }
  }

  /**
   * Populate the catalog with a few items using the REST resource.
   * @throws DuplicateKeyException
   */
  private void populateCatalog(boolean withTimeCol)
  {
    TableMetadata table = TableBuilder.detailTable("trivial", "P1D")
        .build();
    createTableMetadata(table);

    TableBuilder builder = TableBuilder.detailTable("merge", "P1D");
    if (withTimeCol) {
      builder.timeColumn();
    }
    table = builder
        .column("dsa", null) // Use physical type
        .column("dsb", Columns.VARCHAR) // Override types
        .column("dsc", Columns.BIGINT)
        .column("dsd", Columns.FLOAT)
        .column("dse", Columns.DOUBLE)
        .column("dsi", null) // Both null
        .column("newa", null) // Catalog only, no type
        .column("newb", Columns.VARCHAR) // Catalog only, defined type
        .column("newc", Columns.BIGINT)
        .column("newd", Columns.FLOAT)
        .column("newe", Columns.DOUBLE)
        .hiddenColumns(Arrays.asList("dsf", "dsg"))
        .build();
    createTableMetadata(table);
  }

  private PhysicalDatasourceMetadata mockDatasource()
  {
    RowSignature sig = RowSignature.builder()
        .add(Columns.TIME_COLUMN, ColumnType.LONG)
        .add("dsa", ColumnType.DOUBLE)
        .add("dsb", ColumnType.LONG)
        .add("dsc", ColumnType.STRING)
        .add("dsd", ColumnType.LONG)
        .add("dse", ColumnType.FLOAT)
        .add("dsf", ColumnType.STRING)
        .add("dsg", ColumnType.LONG)
        .add("dsh", ColumnType.DOUBLE)
        .add("dsi", null) // In catalog, but null type
        .add("dsj", null) // Not in catalog
        .build();
    return new PhysicalDatasourceMetadata(
        new TableDataSource("merge"),
        sig,
        true,
        true,
        null
    );
  }

  @Test
  public void testUnknownTable()
  {
    // No catalog, no datasource
    assertNull(resolver.resolveDatasource("bogus", null));

    // No catalog entry
    PhysicalDatasourceMetadata dsMetadata = mockDatasource();
    DruidTable table = resolver.resolveDatasource("merge", dsMetadata);
    assertSame(dsMetadata.rowSignature(), table.getRowSignature());
  }

  @Test
  public void testKnownTableNoTime()
  {
    populateCatalog(false);

    // Catalog, no datasource
    DruidTable table = resolver.resolveDatasource("merge", null);
    assertEquals(12, table.getRowSignature().size());
    assertEquals("merge", ((TableDataSource) table.getDataSource()).getName());

    // Spot check
    assertColumnEquals(table, 0, Columns.TIME_COLUMN, ColumnType.LONG);
    assertColumnEquals(table, 1, "dsa", ColumnType.STRING);
    assertColumnEquals(table, 2, "dsb", ColumnType.STRING);
    assertColumnEquals(table, 3, "dsc", ColumnType.LONG);
    assertColumnEquals(table, 6, "dsi", ColumnType.STRING);

    // Catalog, with datasource, result is merged
    // Catalog has no time column
    PhysicalDatasourceMetadata dsMetadata = mockDatasource();
    table = resolver.resolveDatasource("merge", dsMetadata);
    assertEquals(14, table.getRowSignature().size());
    assertSame(dsMetadata.dataSource(), table.getDataSource());
    assertEquals(dsMetadata.isBroadcast(), table.isBroadcast());
    assertEquals(dsMetadata.isJoinable(), table.isJoinable());

    // dsa uses Druid's type, others coerce the type
    assertColumnEquals(table, 0, "dsa", ColumnType.DOUBLE);
    assertColumnEquals(table, 1, "dsb", ColumnType.STRING);
    assertColumnEquals(table, 2, "dsc", ColumnType.LONG);
    assertColumnEquals(table, 3, "dsd", ColumnType.FLOAT);
    assertColumnEquals(table, 4, "dse", ColumnType.DOUBLE);
    assertColumnEquals(table, 5, "dsi", ColumnType.STRING);
    assertColumnEquals(table, 6, "newa", ColumnType.STRING);
    assertColumnEquals(table, 10, "newe", ColumnType.DOUBLE);
    assertColumnEquals(table, 11, Columns.TIME_COLUMN, ColumnType.LONG);
    assertColumnEquals(table, 12, "dsh", ColumnType.DOUBLE);
    assertColumnEquals(table, 13, "dsj", ColumnType.STRING);
  }

  @Test
  public void testKnownTableWithTime()
  {
    populateCatalog(true);

    // Catalog, no datasource
    DruidTable table = resolver.resolveDatasource("merge", null);
    assertEquals(12, table.getRowSignature().size());
    assertEquals("merge", ((TableDataSource) table.getDataSource()).getName());

    // Spot check
    assertColumnEquals(table, 0, Columns.TIME_COLUMN, ColumnType.LONG);
    assertColumnEquals(table, 1, "dsa", ColumnType.STRING);
    assertColumnEquals(table, 2, "dsb", ColumnType.STRING);
    assertColumnEquals(table, 3, "dsc", ColumnType.LONG);
    assertColumnEquals(table, 6, "dsi", ColumnType.STRING);
    assertColumnEquals(table, 7, "newa", ColumnType.STRING);
    assertColumnEquals(table, 11, "newe", ColumnType.DOUBLE);

    // Catalog, with datasource, result is merged
    PhysicalDatasourceMetadata dsMetadata = mockDatasource();
    table = resolver.resolveDatasource("merge", dsMetadata);
    assertEquals(14, table.getRowSignature().size());
    assertSame(dsMetadata.dataSource(), table.getDataSource());
    assertEquals(dsMetadata.isBroadcast(), table.isBroadcast());
    assertEquals(dsMetadata.isJoinable(), table.isJoinable());

    assertColumnEquals(table, 0, Columns.TIME_COLUMN, ColumnType.LONG);
    // dsa uses Druid's type, others coerce the type
    assertColumnEquals(table, 1, "dsa", ColumnType.DOUBLE);
    assertColumnEquals(table, 2, "dsb", ColumnType.STRING);
    assertColumnEquals(table, 3, "dsc", ColumnType.LONG);
    assertColumnEquals(table, 4, "dsd", ColumnType.FLOAT);
    assertColumnEquals(table, 5, "dse", ColumnType.DOUBLE);
    assertColumnEquals(table, 6, "dsi", ColumnType.STRING);
    assertColumnEquals(table, 7, "newa", ColumnType.STRING);
    assertColumnEquals(table, 11, "newe", ColumnType.DOUBLE);
    assertColumnEquals(table, 12, "dsh", ColumnType.DOUBLE);
    assertColumnEquals(table, 13, "dsj", ColumnType.STRING);
  }

  private void assertColumnEquals(DruidTable table, int i, String name, ColumnType type)
  {
    RowSignature sig = table.getRowSignature();
    assertEquals(name, sig.getColumnName(i));
    assertEquals(type, sig.getColumnType(i).orElse(null));
  }
}
