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

package org.apache.druid.testsEx.catalog;

import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;
import org.apache.druid.catalog.guice.Catalog;
import org.apache.druid.catalog.specs.CatalogUtils;
import org.apache.druid.catalog.specs.TableId;
import org.apache.druid.catalog.storage.DatasourceSpec;
import org.apache.druid.catalog.storage.MoveColumn;
import org.apache.druid.catalog.storage.TableMetadata;
import org.apache.druid.catalog.storage.DatasourceColumnSpec.DetailColumnSpec;
import org.apache.druid.testsEx.cluster.CatalogClient;
import org.apache.druid.testsEx.cluster.DruidClusterClient;
import org.apache.druid.testsEx.config.DruidTestRunner;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Light sanity check of the Catalog REST API. Functional testing is
 * done via a unit test. Here we simply ensure that the Jersey plumbing
 * works as intended.
 */
@RunWith(DruidTestRunner.class)
@Category(Catalog.class)
public class ITCatalogRestTest
{
  @Inject
  private DruidClusterClient clusterClient;

  /**
   * Sample a few error cases to ensure the plumbing works.
   * Complete error testing appears in unit tests.
   */
  @Test
  public void testErrors()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();

    // Bogus schema
    assertThrows(
        Exception.class,
        () -> client.createTable(TableId.of("bogus", "foo"), dsSpec)
    );

    // Read-only schema
    assertThrows(
        Exception.class,
        () -> client.createTable(TableId.of(TableId.SYSTEM_SCHEMA, "foo"), dsSpec)
    );

    // Malformed table name
    assertThrows(
        Exception.class,
        () -> client.createTable(TableId.of(TableId.DRUID_SCHEMA, " foo "), dsSpec)
    );
  }

  /**
   * Run though a table lifecycle to sanity check each API. Thorough
   * testing of each API appears in unit tests.
   */
  @Test
  public void testLifecycle()
  {
    CatalogClient client = new CatalogClient(clusterClient);

    // Create a datasource
    TableId tableId = TableId.datasource("example");
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();
    long version = client.createTable(tableId, dsSpec);

    // Update the datasource
    DatasourceSpec dsSpec2 = dsSpec.toBuilder()
        .targetSegmentRows(3_000_000)
        .column("d", "DOUBLE")
        .build();

    Map<String, Object> update = ImmutableMap.of(
        "targetSegmentRows", 3_000_000,
        "columns", Collections.singletonList(new DetailColumnSpec("d", "Double", null))
    );

    // First, optimistic locking, wrong version
    client.updateTable(tableId, update, 1);

    // Optimistic locking, correct version
    long newVersion = client.updateTable(tableId, update, version);
    assertTrue(newVersion > version);

    // Verify the update
    TableMetadata table = client.readTable(tableId);
    assertEquals(dsSpec2, table.spec());

    // Move a column
    MoveColumn moveCmd = new MoveColumn("d", MoveColumn.Position.BEFORE, "a");
    client.moveColumn(tableId, moveCmd);

    // Drop a column
    client.dropColumns(tableId, Collections.singletonList("b"));
    table = client.readTable(tableId);
    assertEquals(Arrays.asList("d", "a", "c"), CatalogUtils.columnNames(((DatasourceSpec) table.spec()).columns()));

    // List schemas
    List<String> schemaNames = client.listSchemas();
    assertTrue(schemaNames.contains(TableId.DRUID_SCHEMA));
    assertTrue(schemaNames.contains(TableId.INPUT_SCHEMA));
    assertTrue(schemaNames.contains(TableId.SYSTEM_SCHEMA));
    assertTrue(schemaNames.contains(TableId.CATALOG_SCHEMA));

    // List table names in schema
    List<String> tableNames = client.listTableNamesInSchema(TableId.DRUID_SCHEMA);
    assertTrue(tableNames.contains(tableId.name()));

    // List tables
    List<TableId> tables = client.listTables();
    assertTrue(tables.contains(tableId));

    // Drop the table
    client.dropTable(tableId);
    tableNames = client.listTableNamesInSchema(TableId.DRUID_SCHEMA);
    assertFalse(tableNames.contains(tableId.name()));
  }
}
