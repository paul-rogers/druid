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

package org.apache.druid.server.http;

import org.apache.druid.catalog.CatalogUtils;
import org.apache.druid.catalog.DatasourceColumnSpec;
import org.apache.druid.catalog.DatasourceSpec;
import org.apache.druid.catalog.HideColumns;
import org.apache.druid.catalog.MoveColumn;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class CommandTest
{
  @Test
  public void testMoveColumn()
  {
    DatasourceSpec dsSpec = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", "VARCHAR")
        .column("b", "BIGINT")
        .column("c", "FLOAT")
        .build();

    // Move first
    MoveColumn cmd = new MoveColumn("c", MoveColumn.Position.FIRST, null);
    List<DatasourceColumnSpec> revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("c", "a", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move last
    cmd = new MoveColumn("a", MoveColumn.Position.LAST, null);
    revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("b", "c", "a"),
        CatalogUtils.columnNames(revised)
    );

    // Move before, earlier anchor
    cmd = new MoveColumn("c", MoveColumn.Position.BEFORE, "b");
    revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("a", "c", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move before, later anchor
    cmd = new MoveColumn("a", MoveColumn.Position.BEFORE, "c");
    revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("b", "a", "c"),
        CatalogUtils.columnNames(revised)
    );

    // Move after, earlier anchor
    cmd = new MoveColumn("c", MoveColumn.Position.AFTER, "a");
    revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("a", "c", "b"),
        CatalogUtils.columnNames(revised)
    );

    // Move after, later anchor
    cmd = new MoveColumn("a", MoveColumn.Position.AFTER, "b");
    revised = cmd.perform(dsSpec.columns());
    assertEquals(
        Arrays.asList("b", "a", "c"),
        CatalogUtils.columnNames(revised)
    );
  }

  @Test
  public void testHideColumns()
  {
    // Everything is null
    HideColumns cmd = new HideColumns(null, null);
    List<String> revised = cmd.perform(null);
    assertNull(revised);

    // Unhide from null list
    cmd = new HideColumns(null, Collections.singletonList("a"));
    revised = cmd.perform(null);
    assertNull(revised);

    // And from an empty list
    cmd = new HideColumns(null, Collections.singletonList("a"));
    revised = cmd.perform(Collections.emptyList());
    assertNull(revised);

    // Hide starting from a null list.
    cmd = new HideColumns(Arrays.asList("a", "b"), null);
    revised = cmd.perform(null);
    assertEquals(Arrays.asList("a", "b"), revised);

    // Hide with existing columns
    cmd = new HideColumns(Arrays.asList("b", "d"), null);
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "c", "d"), revised);

    // Hide starting from an empty list.
    cmd = new HideColumns(Arrays.asList("a", "b"), Collections.emptyList());
    revised = cmd.perform(Collections.emptyList());
    assertEquals(Arrays.asList("a", "b"), revised);

    // Hide with existing columns
    cmd = new HideColumns(null, Arrays.asList("b", "d"));
    revised = cmd.perform(Arrays.asList("a", "b", "c"));
    assertEquals(Arrays.asList("a", "b", "c", "d"), revised);

    // Unhide existing columns

    // Both hide and unhide. Unhide takes precedence.
  }
}
