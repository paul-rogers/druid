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
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.specs.table.DatasourceDefn;
import org.apache.druid.catalog.specs.table.TableDefnRegistry;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

/**
 * Test of validation and serialization of the catalog table definitions.
 */
@Category(CatalogTest.class)
public class DatasourceTableTest
{
  private static final String SUM_BIGINT = "SUM(BIGINT)";

  private final ObjectMapper mapper = new ObjectMapper();
  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testMinimalSpec()
  {
    // Minimum possible definition
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D"
    );
    {
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertSame(TableDefnRegistry.DETAIL_DATASOURCE_DEFN, table.defn());
      table.validate();
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      ResolvedTable table = registry.resolve(spec);
      assertNotNull(table);
      assertSame(TableDefnRegistry.ROLLUP_DATASOURCE_DEFN, table.defn());
      table.validate();
    }
  }

  private void expectValidationFails(final ResolvedTable table)
  {
    assertThrows(IAE.class, () -> table.validate());
  }

  private void expectValidationFails(final TableSpec spec)
  {
    ResolvedTable table = registry.resolve(spec);
    expectValidationFails(table);
  }

  private void expectValidationSucceeds(final TableSpec spec)
  {
    ResolvedTable table = registry.resolve(spec);
    table.validate();
  }

  @Test
  public void testEmptySpec()
  {
    {
      TableSpec spec = new TableSpec(null, ImmutableMap.of(), null);
      assertThrows(IAE.class, () -> registry.resolve(spec));
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, ImmutableMap.of(), null);
      ResolvedTable table = registry.resolve(spec);
      expectValidationFails(table);
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, ImmutableMap.of(), null);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testAllProperties()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(DatasourceDefn.DESCRIPTION_PROPERTY, "My table")
        .put(DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D")
        .put(DatasourceDefn.ROLLUP_GRANULARITY_FIELD, "PT1M")
        .put(DatasourceDefn.TARGET_SEGMENT_ROWS_FIELD, 1_000_000)
        .put(DatasourceDefn.HIDDEN_COLUMNS_FIELD, Arrays.asList("foo", "bar"))
        .build();

    {
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);
      expectValidationSucceeds(spec);

      // Check serialization
      byte[] bytes = spec.toBytes(mapper);
      assertEquals(spec, TableSpec.fromBytes(mapper, bytes));
    }

    {
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationSucceeds(spec);

      // Check serialization
      byte[] bytes = spec.toBytes(mapper);
      assertEquals(spec, TableSpec.fromBytes(mapper, bytes));
    }
  }

  @Test
  public void testWrongTypes()
  {
    {
      TableSpec spec = new TableSpec("bogus", ImmutableMap.of(), null);
      assertThrows(IAE.class, () -> registry.resolve(spec));
    }

    // Segment granularity
    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "bogus"
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }

    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "bogus"
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }

    // Rollup granularity
    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
          DatasourceDefn.ROLLUP_GRANULARITY_FIELD, "bogus"
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }

    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
          DatasourceDefn.ROLLUP_GRANULARITY_FIELD, 10
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }

    // Target segment rows
    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
          DatasourceDefn.TARGET_SEGMENT_ROWS_FIELD, "bogus"
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }

    // Hidden columns
    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
          DatasourceDefn.HIDDEN_COLUMNS_FIELD, "bogus"
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }
    {
      Map<String, Object> props = ImmutableMap.of(
          DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
          DatasourceDefn.HIDDEN_COLUMNS_FIELD, Arrays.asList("a", Columns.TIME_COLUMN)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, null);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testExtendedProperties()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D",
        "foo", 10,
        "bar", "mumble"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);
    expectValidationSucceeds(spec);
  }

  @Test
  public void testColumnSpec()
  {
    // Type is required
    {
      ColumnSpec spec = new ColumnSpec(null, null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }

    // Name is required
    {
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, null, null, null);
      assertThrows(IAE.class, () -> spec.validate());
    }
    {
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", null, null);
      spec.validate();
    }

    // Type is optional
    {
      ColumnSpec spec = new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", "VARCHAR", null);
      spec.validate();
    }
  }

  @Test
  public void testDetailTableColumns()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D"
    );

    // OK to have no columm type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", null, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }

    // Time column can have no type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, Columns.TIME_COLUMN, null, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }

    // Time column can only have TIMESTAMP type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, Columns.TIME_COLUMN, Columns.TIMESTAMP, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, Columns.TIME_COLUMN, Columns.VARCHAR, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Can have a legal scalar type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", Columns.VARCHAR, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }

    // Reject an unknown SQL type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", "BOGUS", null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Cannot use a measure type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Cannot use a measure
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Reject duplicate columns
    {
      List<ColumnSpec> cols = Arrays.asList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", Columns.VARCHAR, null),
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "bar", Columns.BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }
    {
      List<ColumnSpec> cols = Arrays.asList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", Columns.VARCHAR, null),
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", Columns.BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testRollupTableColumns()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D"
    );

    // OK for a dimension to have no type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", null, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }

    // Dimensions must have a scalar type, if the type is non-null
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", Columns.VARCHAR, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", "BOGUS", null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Time column can be a dimension and can only have TIMESTAMP type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, Columns.TIME_COLUMN, Columns.TIMESTAMP, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, Columns.TIME_COLUMN, Columns.VARCHAR, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Measures must have an aggregate type
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "foo", null, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "foo", Columns.VARCHAR, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }

    // Cannot use a detail column
    {
      List<ColumnSpec> cols = Collections.singletonList(
          new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "foo", null, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }

    // Reject duplicate columns
    {
      List<ColumnSpec> cols = Arrays.asList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", Columns.VARCHAR, null),
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "bar", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationSucceeds(spec);
    }
    {
      List<ColumnSpec> cols = Arrays.asList(
          new ColumnSpec(DatasourceDefn.DIMENSION_TYPE, "foo", Columns.VARCHAR, null),
          new ColumnSpec(DatasourceDefn.MEASURE_TYPE, "foo", SUM_BIGINT, null)
      );
      TableSpec spec = new TableSpec(DatasourceDefn.ROLLUP_DATASOURCE_TYPE, props, cols);
      expectValidationFails(spec);
    }
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(ColumnSpec.class)
                  .usingGetClass()
                  .verify();
    EqualsVerifier.forClass(TableSpec.class)
                  .usingGetClass()
                  .verify();
  }

  private TableSpec exampleSpec()
  {
    Map<String, Object> props = ImmutableMap.<String, Object>builder()
        .put(DatasourceDefn.DESCRIPTION_PROPERTY, "My table")
        .put(DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "PT1H")
        .put(DatasourceDefn.ROLLUP_GRANULARITY_FIELD, "PT1M")
        .put(DatasourceDefn.TARGET_SEGMENT_ROWS_FIELD, 1_000_000)
        .put(DatasourceDefn.HIDDEN_COLUMNS_FIELD, Arrays.asList("foo", "bar"))
        .put("tag1", "some value")
        .put("tag2", "second value")
        .build();
    Map<String, Object> colProps = ImmutableMap.<String, Object>builder()
        .put("colProp1", "value 1")
        .put("colProp2", "value 2")
        .build();
    List<ColumnSpec> cols = Arrays.asList(
        new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "a", null, colProps),
        new ColumnSpec(DatasourceDefn.DETAIL_COLUMN_TYPE, "b", Columns.VARCHAR, null)
    );
    TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, cols);

    // Sanity check
    expectValidationSucceeds(spec);
    return spec;
  }

  @Test
  public void testSerialization()
  {
    TableSpec spec = exampleSpec();

    // Round-trip
    TableSpec spec2 = TableSpec.fromBytes(mapper, spec.toBytes(mapper));
    assertEquals(spec, spec2);

    // Sanity check of toString, which uses JSON
    assertNotNull(spec.toString());
  }

  private TableSpec mergeTables(TableSpec spec, TableSpec update)
  {
    ResolvedTable table = registry.resolve(spec);
    assertNotNull(table);
    return table.merge(update).spec();
  }

  @Test
  public void testMergeEmpty()
  {
    TableSpec spec = exampleSpec();
    TableSpec update = new TableSpec(null, null, null);

    TableSpec merged = mergeTables(spec, update);
    assertEquals(spec, merged);
  }

  private void assertMergeFails(TableSpec spec, TableSpec update)
  {
    assertThrows(IAE.class, () -> mergeTables(spec, update));
  }

  @Test
  public void testMergeTableType()
  {
    TableSpec spec = exampleSpec();

    // Null type test is above.
    // Wrong type
    TableSpec update = new TableSpec("bogus", null, null);
    assertMergeFails(spec, update);

    // Same type
    update = new TableSpec(spec.type(), null, null);
    TableSpec merged = mergeTables(spec, update);
    assertEquals(spec, merged);
  }

  @Test
  public void testMergeProperties()
  {
    TableSpec spec = exampleSpec();

    // Use a regular map, not an immutable one, because immutable maps,
    // in their infinite wisdom, don't allow null values. But, we need
    // such values to indicate which properties to remove.
    Map<String, Object> updatedProps = new HashMap<>();
    // Update a property
    updatedProps.put(DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D");
    // Remove a property
    updatedProps.put("tag1", null);
    // Add a property
    updatedProps.put("tag3", "third value");

    TableSpec update = new TableSpec(null, updatedProps, null);
    TableSpec merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify those that we
    // changed.
    assertNotEquals(spec, merged);
    assertEquals(
        updatedProps.get(DatasourceDefn.SEGMENT_GRANULARITY_FIELD),
        merged.properties().get(DatasourceDefn.SEGMENT_GRANULARITY_FIELD)
    );
    assertFalse(merged.properties().containsKey("tag1"));
    assertEquals(
        updatedProps.get("tag3"),
        merged.properties().get("tag3")
    );
  }

  @Test
  public void testMergeHiddenCols()
  {
    TableSpec spec = exampleSpec();

    // Remove all hidden columns
    Map<String, Object> updatedProps = new HashMap<>();
    updatedProps.put(DatasourceDefn.HIDDEN_COLUMNS_FIELD, null);
    TableSpec update = new TableSpec(null, updatedProps, null);
    TableSpec merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);
    assertFalse(
        merged.properties().containsKey(DatasourceDefn.HIDDEN_COLUMNS_FIELD)
    );

    // Wrong type
    updatedProps = ImmutableMap.of(
        DatasourceDefn.HIDDEN_COLUMNS_FIELD, "mumble"
    );
    update = new TableSpec(null, updatedProps, null);
    assertMergeFails(spec, update);

    // Merge
    updatedProps = ImmutableMap.of(
        DatasourceDefn.HIDDEN_COLUMNS_FIELD, Collections.singletonList("mumble")
    );
    update = new TableSpec(null, updatedProps, null);
    merged = mergeTables(spec, update);
    expectValidationSucceeds(merged);

    assertEquals(
        Arrays.asList("foo", "bar", "mumble"),
        merged.properties().get(DatasourceDefn.HIDDEN_COLUMNS_FIELD)
    );
  }

  @Test
  public void testMergeColsWithEmptyList()
  {
    Map<String, Object> props = ImmutableMap.of(
        DatasourceDefn.SEGMENT_GRANULARITY_FIELD, "P1D"
    );
    TableSpec spec = new TableSpec(DatasourceDefn.DETAIL_DATASOURCE_TYPE, props, null);

    List<ColumnSpec> colUpdates = Collections.singletonList(
        new ColumnSpec(
            DatasourceDefn.DETAIL_COLUMN_TYPE,
            "a",
            Columns.BIGINT,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.BIGINT, columns.get(0).sqlType());
  }

  @Test
  public void testMergeCols()
  {
    TableSpec spec = exampleSpec();

    Map<String, Object> updatedProps = new HashMap<>();
    // Update a property
    updatedProps.put("colProp1", "new value");
    // Remove a property
    updatedProps.put("colProp2", null);
    // Add a property
    updatedProps.put("tag3", "third value");

    List<ColumnSpec> colUpdates = Arrays.asList(
        new ColumnSpec(
            DatasourceDefn.DETAIL_COLUMN_TYPE,
            "a",
            Columns.BIGINT,
            updatedProps
        ),
        new ColumnSpec(
            DatasourceDefn.DETAIL_COLUMN_TYPE,
            "c",
            Columns.VARCHAR,
            null
        )
    );
    TableSpec update = new TableSpec(null, null, colUpdates);
    TableSpec merged = mergeTables(spec, update);

    assertNotEquals(spec, merged);
    List<ColumnSpec> columns = merged.columns();
    assertEquals(3, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals(Columns.BIGINT, columns.get(0).sqlType());
    Map<String, Object> colProps = columns.get(0).properties();
    assertEquals(2, colProps.size());
    assertEquals("new value", colProps.get("colProp1"));
    assertEquals("third value", colProps.get("tag3"));

    assertEquals("c", columns.get(2).name());
    assertEquals(Columns.VARCHAR, columns.get(2).sqlType());
  }
}
