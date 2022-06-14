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

package org.apache.druid.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import org.apache.druid.catalog.AbstractColumnMetadata.InputColumn;
import org.apache.druid.catalog.AbstractColumnMetadata.MeasureColumn;
import org.apache.druid.catalog.AbstractTableMetadata.DatasourceTable;
import org.apache.druid.catalog.AbstractTableMetadata.InputSourceTable;
import org.apache.druid.catalog.MetadataCatalog.ColumnKind;
import org.apache.druid.catalog.MetadataCatalog.ColumnMetadata;
import org.apache.druid.catalog.MetadataCatalog.TableMetadata;
import org.apache.druid.catalog.MetadataCatalog.TableType;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.metadata.catalog.CatalogManager.DuplicateKeyException;
import org.apache.druid.metadata.catalog.CatalogManager.OutOfDateException;
import org.apache.druid.segment.column.ColumnType;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class MetadataCatalogTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogStorage storage;
  private ObjectMapper jsonMapper;
  private ObjectMapper smileMapper;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    storage = dbFixture.storage;
    jsonMapper = new ObjectMapper();
    smileMapper = new ObjectMapper(new SmileFactory());
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  @Test
  public void testDirect() throws DuplicateKeyException, OutOfDateException
  {
    populateCatalog();
    MetadataCatalog catalog = new LocalMetadataCatalog(storage, storage.schemaRegistry);
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);
  }

  @Test
  public void testCached() throws DuplicateKeyException, OutOfDateException
  {
    populateCatalog();
    CachedMetadataCatalog catalog = new CachedMetadataCatalog(storage, storage.schemaRegistry);
    storage.register(catalog);
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);

    // Also test the deletion case
    TableId table2 = TableId.datasource("table2");
    storage.tables().delete(table2);
    assertNull(storage.tables().read(table2));

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table3", tables.get(1).id().name());
  }

  @Test
  public void testRemoteWithJson() throws DuplicateKeyException, OutOfDateException
  {
    doTestRemote(false);
  }

  @Test
  public void testRemoteWithSmile() throws DuplicateKeyException, OutOfDateException
  {
    doTestRemote(true);
  }

  private void doTestRemote(boolean useSmile) throws DuplicateKeyException, OutOfDateException
  {
    populateCatalog();
    MockCatalogSync sync = new MockCatalogSync(storage, jsonMapper, smileMapper, useSmile);
    MetadataCatalog catalog = sync.catalog();
    storage.register(sync);
    verifyInitial(catalog);
    alterCatalog();
    verifyAltered(catalog);

    // Also test the deletion case
    TableId table2 = TableId.datasource("table2");
    storage.tables().delete(table2);
    assertNull(storage.tables().read(table2));

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table3", tables.get(1).id().name());
  }

  /**
   * Populate the catalog with a few items using the REST resource.
   * @throws DuplicateKeyException
   */
  private void populateCatalog() throws DuplicateKeyException
  {
    DatasourceDefn defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .timeColumn()
        .column("a", "VARCHAR")
        .build();
    TableSpec table = TableSpec.newTable(
        TableId.DRUID_SCHEMA,
        "table1",
        defn);
    storage.tables().create(table);

    defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .rollupGranularity("PT1H")
        .timeColumn()
        .column("dim", "VARCHAR")
        .measure("measure", "BIGINT", "SUM")
        .build();
    table = TableSpec.newTable(
        TableId.DRUID_SCHEMA,
        "table2",
        defn);
    storage.tables().create(table);

    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = CatalogTests.csvFormat();
    InputSourceDefn inputDefn = InputSourceDefn
        .builder()
        .source(inputSource)
        .format(inputFormat)
        .column("a", "varchar")
        .build();
    table = TableSpec.newTable(
        TableId.INPUT_SCHEMA,
        "input",
        inputDefn);
    storage.tables().create(table);
  }

  private void verifyInitial(MetadataCatalog catalog)
  {
    {
      TableId id = TableId.datasource("table1");
      TableMetadata table = catalog.resolveTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);
      assertEquals(TableType.DATASOURCE, table.type());

      List<ColumnMetadata> cols = table.columns();
      assertEquals(2, cols.size());
      assertEquals("__time", cols.get(0).name());
      assertEquals("TIMESTAMP", cols.get(0).sqlType());
      assertEquals(ColumnKind.SIMPLE, cols.get(0).kind());
      assertEquals("a", cols.get(1).name());
      assertEquals("VARCHAR", cols.get(1).sqlType());
      assertEquals(ColumnKind.SIMPLE, cols.get(0).kind());
      assertSame(cols.get(0), table.column("__time"));
      assertSame(cols.get(1), table.column("a"));
      assertNull(table.column("b"));

      DatasourceTable dsTable = (DatasourceTable) table;
      assertEquals("PT1D", dsTable.segmentGranularity());
      assertTrue(dsTable.isDetail());
      assertFalse(dsTable.isRollup());
      assertNull(dsTable.rollupGranularity());
    }
    {
      TableId id = TableId.datasource("table2");
      TableMetadata table = catalog.resolveTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);
      assertEquals(TableType.DATASOURCE, table.type());

      List<ColumnMetadata> cols = table.columns();
      assertEquals(3, cols.size());
      assertEquals("__time", cols.get(0).name());
      assertEquals("TIMESTAMP", cols.get(0).sqlType());
      assertEquals(ColumnKind.DIMENSION, cols.get(0).kind());
      assertEquals("dim", cols.get(1).name());
      assertEquals("VARCHAR", cols.get(1).sqlType());
      assertEquals(ColumnKind.DIMENSION, cols.get(1).kind());
      assertEquals("measure", cols.get(2).name());
      assertEquals("BIGINT", cols.get(2).sqlType());
      assertEquals(ColumnKind.MEASURE, cols.get(2).kind());
      assertEquals("SUM", ((MeasureColumn) cols.get(2)).aggFn());
      assertSame(cols.get(0), table.column("__time"));
      assertSame(cols.get(1), table.column("dim"));
      assertSame(cols.get(2), table.column("measure"));

      DatasourceTable dsTable = (DatasourceTable) table;
      assertEquals("PT1D", dsTable.segmentGranularity());
      assertFalse(dsTable.isDetail());
      assertTrue(dsTable.isRollup());
      assertEquals("PT1H", dsTable.rollupGranularity());
    }
    assertNull(catalog.resolveTable(TableId.datasource("table3")));
    {
      TableId id = TableId.inputSource("input");
      TableMetadata table = catalog.resolveTable(id);
      assertEquals(id, table.id());
      assertTrue(table.updateTime() > 0);
      assertEquals(TableType.INPUT, table.type());

      List<ColumnMetadata> cols = table.columns();
      assertEquals(1, cols.size());
      assertEquals("a", cols.get(0).name());
      assertEquals("varchar", cols.get(0).sqlType());
      assertEquals(ColumnKind.INPUT, cols.get(0).kind());
      assertEquals(ColumnType.STRING, ((InputColumn) cols.get(0)).druidType());

      InputSourceTable inputTable = (InputSourceTable) table;
      assertNotNull(inputTable.inputSource());
      assertNotNull(inputTable.format());
    }

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(2, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table2", tables.get(1).id().name());

    tables = catalog.tables(TableId.INPUT_SCHEMA);
    assertEquals(1, tables.size());
    assertEquals("input", tables.get(0).id().name());
  }

  private void alterCatalog() throws DuplicateKeyException, OutOfDateException
  {
    // Add a column to table 1
    TableId id1 = TableId.datasource("table1");
    TableSpec table1 = storage.tables().read(id1);
    assertNotNull(table1);

    DatasourceDefn defn = (DatasourceDefn) table1.defn();
    defn = defn.toBuilder()
        .column("b", "DOUBLE")
        .build();
    storage.tables().updateDefn(id1, defn, table1.updateTime());

    // Create a table 3
    defn = DatasourceDefn.builder()
        .segmentGranularity("PT1D")
        .timeColumn()
        .column("x", "FLOAT")
        .build();
    TableSpec table = TableSpec.newTable(
        TableId.DRUID_SCHEMA,
        "table3",
        defn);
    storage.tables().create(table);
  }

  private void verifyAltered(MetadataCatalog catalog)
  {
    {
      TableId id = TableId.datasource("table1");
      TableMetadata table = catalog.resolveTable(id);

      List<ColumnMetadata> cols = table.columns();
      assertEquals(3, cols.size());
      assertEquals("__time", cols.get(0).name());
      assertEquals("a", cols.get(1).name());
      assertEquals("b", cols.get(2).name());
      assertEquals("DOUBLE", cols.get(2).sqlType());
      assertEquals(ColumnKind.SIMPLE, cols.get(2).kind());
      assertSame(cols.get(2), table.column("b"));
    }
    {
      TableId id = TableId.datasource("table3");
      TableMetadata table = catalog.resolveTable(id);

      List<ColumnMetadata> cols = table.columns();
      assertEquals(2, cols.size());
      assertEquals("__time", cols.get(0).name());
      assertEquals("x", cols.get(1).name());
    }

    List<TableMetadata> tables = catalog.tables(TableId.DRUID_SCHEMA);
    assertEquals(3, tables.size());
    assertEquals("table1", tables.get(0).id().name());
    assertEquals("table2", tables.get(1).id().name());
    assertEquals("table3", tables.get(2).id().name());
  }
}
