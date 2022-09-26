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

package org.apache.druid.server.http.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.CatalogTests;
import org.apache.druid.catalog.TableId;
import org.apache.druid.catalog.specs.CatalogUtils;
import org.apache.druid.catalog.storage.DatasourceSpec;
import org.apache.druid.catalog.storage.HideColumns;
import org.apache.druid.catalog.storage.InputTableSpec;
import org.apache.druid.catalog.storage.MoveColumn;
import org.apache.druid.catalog.storage.TableMetadata;
import org.apache.druid.metadata.TestDerbyConnector;
import org.apache.druid.server.http.CatalogResource;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import javax.ws.rs.core.Response;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.apache.druid.server.http.catalog.DummyRequest.deleteBy;
import static org.apache.druid.server.http.catalog.DummyRequest.getBy;
import static org.apache.druid.server.http.catalog.DummyRequest.postBy;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Test of REST API operations for the table catalog.
 */
@Category(CatalogTest.class)
public class CatalogResourceTest
{
  @Rule
  public final TestDerbyConnector.DerbyConnectorRule derbyConnectorRule = new TestDerbyConnector.DerbyConnectorRule();

  private CatalogTests.DbFixture dbFixture;
  private CatalogResource resource;
  private ObjectMapper jsonMapper;

  @Before
  public void setUp()
  {
    dbFixture = new CatalogTests.DbFixture(derbyConnectorRule);
    jsonMapper = new ObjectMapper();
    resource = new CatalogResource(dbFixture.storage, jsonMapper);
  }

  @After
  public void tearDown()
  {
    CatalogTests.tearDown(dbFixture);
  }

  private static long getVersion(Response resp)
  {
    @SuppressWarnings("unchecked")
    Map<String, Object> result = (Map<String, Object>) resp.getEntity();
    return (Long) result.get("version");
  }

  @Test
  public void testCreate()
  {
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    final String tableName = "create";

    // Blank schema name: infer the schema.
    Response resp = resource.postTable("", tableName, dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.postTable(TableId.DRUID_SCHEMA, "", dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Invalid table name
    resp = resource.postTable(TableId.DRUID_SCHEMA, " bogus ", dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.postTable("bogus", tableName, dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.postTable(TableId.CATALOG_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Wrong definition type.
    resp = resource.postTable(TableId.INPUT_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // No permissions
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Read permission
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Write permission
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > 0);

    // Duplicate
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Duplicate, "if not exists"
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "ifnew", 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertEquals(0, getVersion(resp));

    // Input source
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a,b,1\nc,d,2\n"
    );
    InputTableSpec inputSpec = InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .build();
    resp = resource.postTable(TableId.INPUT_SCHEMA, "input", inputSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Wrong spec type
    resp = resource.postTable(TableId.DRUID_SCHEMA, "invalid", inputSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testUpdate()
  {
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    final String tableName = "update";

    // Does not exist
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "replace", 0, postBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // No update permission
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "replace", 0, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Out-of-date version
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "replace", 10, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Valid version
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "replace", version, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // Overwrite
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "replace", 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
  }

  @Test
  public void testForce()
  {
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    final String tableName = "force";

    // Create the table
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "force", 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Overwrite
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, "force", 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
  }

  @Test
  public void testRead()
  {
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();

    // Missing schema name
    String tableName = "read";
    Response resp = resource.getTable("", tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.getTable(TableId.DRUID_SCHEMA, null, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.getTable("bogus", tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create the table
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // No read permission
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Valid
    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(version, read.updateTime());
    assertEquals(dsSpec, read.spec());

    // Internal sync API
    resp = resource.syncTable(TableId.DRUID_SCHEMA, tableName, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    read = (TableMetadata) resp.getEntity();
    assertEquals(version, read.updateTime());
    assertEquals(dsSpec, read.spec());
  }

  @SuppressWarnings("unchecked")
  private List<TableId> getTableIdList(Response resp)
  {
    return (List<TableId>) resp.getEntity();
  }

  @SuppressWarnings("unchecked")
  private List<String> getTableList(Response resp)
  {
    return (List<String>) resp.getEntity();
  }

  @SuppressWarnings("unchecked")
  private List<TableMetadata> getDetailsList(Response resp)
  {
    return (List<TableMetadata>) resp.getEntity();
  }

  @Test
  public void testList()
  {
    // No entries
    Response resp = resource.listTables(getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableId> tableIds = getTableIdList(resp);
    assertTrue(tableIds.isEmpty());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<String> tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Missing schema
    resp = resource.listTables(null, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Invalid schema
    resp = resource.listTables("bogus", getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Create a table
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    resp = resource.postTable(TableId.DRUID_SCHEMA, "list", dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No read access
    resp = resource.listTables(getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertTrue(tableIds.isEmpty());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.DENY_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Read access
    resp = resource.listTables(getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertEquals(1, tableIds.size());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(1, tables.size());

    resp = resource.listTables(TableId.SYSTEM_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertTrue(tables.isEmpty());

    // Internal sync schema API
    resp = resource.syncSchema(TableId.SYSTEM_SCHEMA, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getDetailsList(resp).isEmpty());

    resp = resource.syncSchema(TableId.DRUID_SCHEMA, getBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableMetadata> details = getDetailsList(resp);
    assertEquals(1, details.size());
  }

  @Test
  public void testDelete()
  {
    // Missing schema name
    String tableName = "delete";
    Response resp = resource.deleteTable("", tableName, false, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Missing table name
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, null, false, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Unknown schema
    resp = resource.deleteTable("bogus", tableName, false, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Immutable schema
    resp = resource.deleteTable(TableId.CATALOG_SCHEMA, tableName, false, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Does not exist
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, true, deleteBy(CatalogTests.SUPER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // Create the table
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    // No write permission
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.FORBIDDEN.getStatusCode(), resp.getStatus());

    // Write permission
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, false, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, tableName, true, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
  }

  @Test
  public void testLifecycle()
  {
    // Operations for one table - create
    String table1Name = "lifecycle1";
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, table1Name, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // read
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read1 = (TableMetadata) resp.getEntity();
    TableId id1 = TableId.of(TableId.DRUID_SCHEMA, table1Name);
    assertEquals(id1, read1.id());
    assertEquals(version, read1.updateTime());
    assertEquals(dsSpec, read1.spec());

    // list
    resp = resource.listTables(getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<TableId> tableIds = getTableIdList(resp);
    assertEquals(1, tableIds.size());
    assertEquals(id1, tableIds.get(0));

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    List<String> tables = getTableList(resp);
    assertEquals(1, tables.size());
    assertEquals(id1.name(), tables.get(0));

    // update
    DatasourceSpec defn2 = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .build();
    resp = resource.postTable(TableId.DRUID_SCHEMA, table1Name, defn2, "replace", version, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);
    version = getVersion(resp);

    // verify update
    resp = resource.getTable(TableId.DRUID_SCHEMA, table1Name, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(read1.creationTime(), read.creationTime());
    assertEquals(version, read.updateTime());
    assertEquals(defn2, read.spec());

    // add second table
    String table2Name = "lifecycle2";
    resp = resource.postTable(TableId.DRUID_SCHEMA, table2Name, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableId id2 = TableId.of(TableId.DRUID_SCHEMA, table2Name);

    // verify lists
    resp = resource.listTables(getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tableIds = getTableIdList(resp);
    assertEquals(2, tableIds.size());
    assertEquals(id1, tableIds.get(0));
    assertEquals(id2, tableIds.get(1));

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(2, tables.size());
    assertEquals(id1.name(), tables.get(0));
    assertEquals(id2.name(), tables.get(1));

    // delete and verify
    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table1Name, false, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(1, tables.size());

    resp = resource.deleteTable(TableId.DRUID_SCHEMA, table2Name, false, deleteBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.listTables(TableId.DRUID_SCHEMA, getBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    tables = getTableList(resp);
    assertEquals(0, tables.size());
  }

  @Test
  public void testMoveColumn()
  {
    String tableName = "move";
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    MoveColumn cmd = new MoveColumn("foo", MoveColumn.Position.FIRST, null);
    resp = resource.moveColumn("bogus", tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Bad table
    resp = resource.moveColumn(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // No target column
    cmd = new MoveColumn(null, MoveColumn.Position.FIRST, null);
    resp = resource.moveColumn(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // No anchor column
    cmd = new MoveColumn("a", MoveColumn.Position.BEFORE, null);
    resp = resource.moveColumn(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.BAD_REQUEST.getStatusCode(), resp.getStatus());

    // Move first
    cmd = new MoveColumn("c", MoveColumn.Position.FIRST, null);
    resp = resource.moveColumn(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    assertTrue(getVersion(resp) > version);

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(
        Arrays.asList("c", "a", "b"),
        CatalogUtils.columnNames(((DatasourceSpec) read.spec()).columns())
    );

    // Other cases are tested in CommandTest since all the REST plumbing is the same
  }

  @Test
  public void testHideColumns()
  {
    String tableName = "hide";
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    HideColumns cmd = new HideColumns(null, null);
    resp = resource.hideColumns("bogus", tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Bad table
    resp = resource.hideColumns(TableId.DRUID_SCHEMA, "bogus", cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Nothing to do
    resp = resource.hideColumns(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertNull(((DatasourceSpec) read.spec()).hiddenColumns());

    // Hide
    cmd = new HideColumns(Arrays.asList("a", "b"), null);
    resp = resource.hideColumns(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertEquals(Arrays.asList("a", "b"), ((DatasourceSpec) read.spec()).hiddenColumns());
    assertTrue(read.updateTime() > version);

    // Unhide + hide
    cmd = new HideColumns(Arrays.asList("b", "c"), Arrays.asList("a", "e"));
    resp = resource.hideColumns(TableId.DRUID_SCHEMA, tableName, cmd, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertEquals(Arrays.asList("b", "c"), ((DatasourceSpec) read.spec()).hiddenColumns());
    assertTrue(read.updateTime() > version);

    // Other cases are tested in CommandTest
  }

  @Test
  public void testDropColumns()
  {
    String tableName = "drop";
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();

    Response resp = resource.postTable(TableId.DRUID_SCHEMA, tableName, dsSpec, null, 0, postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());
    long version = getVersion(resp);

    // Bad schema
    resp = resource.dropColumns("bogus", tableName, Collections.emptyList(), postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Bad table
    resp = resource.dropColumns(TableId.DRUID_SCHEMA, "bogus", Collections.emptyList(), postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.NOT_FOUND.getStatusCode(), resp.getStatus());

    // Nothing to do
    resp = resource.dropColumns(TableId.DRUID_SCHEMA, tableName, Collections.emptyList(), postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    TableMetadata read = (TableMetadata) resp.getEntity();
    assertEquals(
        CatalogUtils.columnNames(dsSpec.columns()),
        CatalogUtils.columnNames(((DatasourceSpec) read.spec()).columns())
    );

    // Drop
    resp = resource.dropColumns(TableId.DRUID_SCHEMA, tableName, Arrays.asList("a", "c"), postBy(CatalogTests.WRITER_USER));
    assertEquals(Response.Status.OK.getStatusCode(), resp.getStatus());

    resp = resource.getTable(TableId.DRUID_SCHEMA, tableName, postBy(CatalogTests.READER_USER));
    read = (TableMetadata) resp.getEntity();
    assertTrue(read.updateTime() > version);
    assertEquals(
        Collections.singletonList("b"),
        CatalogUtils.columnNames(((DatasourceSpec) read.spec()).columns())
    );
  }
}
