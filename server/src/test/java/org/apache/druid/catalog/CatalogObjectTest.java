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

import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.catalog.CatalogManager.TableState;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

@Category(CatalogTest.class)
public class CatalogObjectTest
{
  @Test
  public void testId()
  {
    TableId id1 = new TableId("schema", "table");
    assertEquals(id1, id1);
    assertEquals("schema", id1.schema());
    assertEquals("table", id1.name());
    assertEquals("\"schema\".\"table\"", id1.sqlName());
    assertEquals(id1.sqlName(), id1.toString());

    TableId id2 = TableId.datasource("ds");
    assertEquals(TableId.DRUID_SCHEMA, id2.schema());
    assertEquals("ds", id2.name());
  }

  @Test
  public void testIdEquals()
  {
    EqualsVerifier.forClass(TableId.class)
                  .usingGetClass()
                  .verify();
  }

  @Test
  public void testMinimalTable()
  {
    DatasourceSpec spec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    {
      TableMetadata table = new TableMetadata(
          TableId.DRUID_SCHEMA,
          "foo",
          10,
          20,
          TableState.ACTIVE,
          spec
      );
      table.validate();
      assertEquals(TableId.DRUID_SCHEMA, table.dbSchema());
      assertEquals("foo", table.name());
      assertEquals(10, table.creationTime());
      assertEquals(20, table.updateTime());
      assertEquals(TableState.ACTIVE, table.state());
      assertNotNull(table.spec());
    }

    {
      TableMetadata table = new TableMetadata(
          null,
          "foo",
          10,
          20,
          TableState.ACTIVE,
          null
      );
      assertThrows(IAE.class, () -> table.validate());
    }

    {
      TableMetadata table = new TableMetadata(
          TableId.DRUID_SCHEMA,
          null,
          10,
          20,
          TableState.ACTIVE,
          null
      );
      assertThrows(IAE.class, () -> table.validate());
    }
  }

  @Test
  public void testSpec()
  {
    DatasourceSpec spec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    TableMetadata table = new TableMetadata(
        TableId.DRUID_SCHEMA,
        "foo",
        10,
        20,
        TableState.ACTIVE,
        spec
    );
    table.validate();
    assertSame(spec, table.spec());

    // Segment grain is required.
    spec = DatasourceSpec.builder()
        .build();
    TableMetadata table2 = new TableMetadata(
        "wrong",
        "foo",
        10,
        20,
        TableState.ACTIVE,
        spec
    );
    assertThrows(IAE.class, () -> table2.validate());
  }

  @Test
  public void testConversions()
  {
    DatasourceSpec spec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();
    TableMetadata table = TableMetadata.newSegmentTable(
        "ds",
        spec
    );
    assertEquals(TableId.datasource("ds"), table.id());
    assertEquals(TableState.ACTIVE, table.state());
    assertEquals(0, table.updateTime());
    assertSame(spec, table.spec());

    TableMetadata table2 = TableMetadata.newSegmentTable("ds", spec);
    assertEquals(table, table2);

    TableMetadata table3 = table2.asUpdate(20);
    assertEquals(20, table3.updateTime());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(TableMetadata.class)
                  .usingGetClass()
                  .verify();
  }
}
