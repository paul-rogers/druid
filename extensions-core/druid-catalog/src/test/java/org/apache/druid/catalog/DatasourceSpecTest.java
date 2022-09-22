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
import com.google.common.collect.ImmutableMap;
import nl.jqno.equalsverifier.EqualsVerifier;
import org.apache.druid.catalog.DatasourceColumnSpec.DimensionSpec;
import org.apache.druid.catalog.DatasourceColumnSpec.MeasureSpec;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test of validation and serialization of the catalog table definitions.
 */
@Category(CatalogTest.class)
public class DatasourceSpecTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .build();

    defn.validate();
    assertEquals("P1D", defn.segmentGranularity());
    assertNull(defn.rollupGranularity());
    assertEquals(0, defn.targetSegmentRows());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testFullBuilder()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .description("My table")
        .hiddenColumns(Arrays.asList("foo", "bar"))
        .build();

    defn.validate();
    assertEquals("My table", defn.description());
    assertEquals("PT1H", defn.segmentGranularity());
    assertEquals("PT1M", defn.rollupGranularity());
    assertEquals(1_000_000, defn.targetSegmentRows());
    assertEquals(Arrays.asList("foo", "bar"), defn.hiddenColumns());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  @Test
  public void testTags()
  {
    Map<String, Object> props = ImmutableMap.of(
        "foo", 10, "bar", "mumble");
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("P1M")
        .tags(props)
        .build();

    defn.validate();
    assertEquals(props, defn.tags());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);
  }

  private void expectValidationFails(final DatasourceSpec defn)
  {
    assertThrows(IAE.class, () -> defn.validate());
  }

  @Test
  public void testColumns()
  {
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .column("a", null)
        .column("b", "VARCHAR")
        .measure("c", "SUM(BIGINT)")
        .build();

    defn.validate();
    List<DatasourceColumnSpec> columns = defn.columns();
    assertEquals(3, columns.size());
    assertTrue(columns.get(0) instanceof DatasourceColumnSpec);
    assertEquals("a", columns.get(0).name());
    assertNull(columns.get(0).sqlType());
    assertTrue(columns.get(1) instanceof DatasourceColumnSpec);
    assertEquals("b", columns.get(1).name());
    assertEquals("VARCHAR", columns.get(1).sqlType());
    assertTrue(columns.get(2) instanceof MeasureSpec);
    assertEquals("c", columns.get(2).name());
    assertEquals("SUM(BIGINT)", columns.get(2).sqlType());
    assertSame(MeasureTypes.SUM_BIGINT_TYPE, ((MeasureSpec) columns.get(2)).measureType());

    DatasourceSpec copy = defn.toBuilder().build();
    assertEquals(defn, copy);

    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("c", "FOO")
        .build();
    expectValidationFails(defn);

    defn = DatasourceSpec.builder()
          .segmentGranularity("P1D")
          .measure("c", "SUM(BIGINT)")
          .build();
    expectValidationFails(defn);

    defn = DatasourceSpec.builder()
          .segmentGranularity("P1D")
          .column("a", null)
          .column("a", null)
          .build();
    expectValidationFails(defn);
  }

  @Test
  public void testTimeColumn()
  {
    // Detail table
    // OK to have no type: Druid will pick TIMESTAMP
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column(Columns.TIME_COLUMN, null)
        .build();

    defn.validate();

    // OK to explicitly state TIMESTAMP
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column(Columns.TIME_COLUMN, Columns.TIMESTAMP)
        .build();

    defn.validate();

    // Cannot give an invalid type
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column(Columns.TIME_COLUMN, Columns.BIGINT)
        .build();
    expectValidationFails(defn);

    // Rollup - Dimension
    // OK to have no type: Druid will pick TIMESTAMP
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .column(Columns.TIME_COLUMN, null)
        .build();

    defn.validate();

    // OK to explicitly state TIMESTAMP
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .column(Columns.TIME_COLUMN, Columns.TIMESTAMP)
        .build();

    defn.validate();

    // Cannot give an invalid type
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .column(Columns.TIME_COLUMN, Columns.BIGINT)
        .build();
    expectValidationFails(defn);

    // Rollup - Invalid as measure
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .measure(Columns.TIME_COLUMN, null)
        .build();
    expectValidationFails(defn);

    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .rollupGranularity("PT1M")
        .measure(Columns.TIME_COLUMN, Columns.TIMESTAMP)
        .build();
    expectValidationFails(defn);
  }

  @Test
  public void testHiddenColumns()
  {
    // Disjoint set - OK
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", null)
        .column("b", "VARCHAR")
        .hiddenColumns(Arrays.asList("c", "d"))
        .build();
    defn.validate();

    // Overlapping sets - not so good
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .column("a", null)
        .column("b", "VARCHAR")
        .hiddenColumns(Arrays.asList("a", "c"))
        .build();
    expectValidationFails(defn);

    // Cannot hide __time
    defn = DatasourceSpec.builder()
        .segmentGranularity("P1D")
        .timeColumn()
        .column("a", null)
        .column("b", "VARCHAR")
        .hiddenColumns(Arrays.asList(Columns.TIME_COLUMN, "c"))
        .build();
    expectValidationFails(defn);
  }

  @Test
  public void testValidation()
  {
    // Ignore rollup grain for detail table
    DatasourceSpec defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .build();

    assertNull(defn.rollupGranularity());
    assertEquals("PT1H", defn.segmentGranularity());

    // Negative segment size mapped to 0
    defn = DatasourceSpec.builder()
        .segmentGranularity("PT1H")
        .targetSegmentRows(-1)
        .build();
    assertEquals(0, defn.targetSegmentRows());
  }

  @Test
  public void testSerialization()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    // Round-trip
    TableSpec defn2 = TableSpec.fromBytes(mapper, defn.toBytes(mapper));
    assertEquals(defn, defn2);

    // Sanity check of toString, which uses JSON
    assertNotNull(defn.toString());
  }

  private DatasourceSpec exampleSpec()
  {
    return DatasourceSpec.builder()
        .description("My Table")
        .segmentGranularity("PT1H")
        .rollupGranularity("PT1M")
        .targetSegmentRows(1_000_000)
        .column("a", null)
        .column("b", "VARCHAR")
        .hiddenColumns(Arrays.asList("foo", "bar"))
        .tag("tag1", "some value")
        .tag("tag2", "second value")
        .build();
  }

  @Test
  public void testMergeEmpty()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    // Create an update that mirrors what REST will do.
    Map<String, Object> raw = new HashMap<>();
    raw.put("type", DatasourceSpec.JSON_TYPE);
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof DatasourceSpec);

    TableSpec merged = defn.merge(update, raw, mapper);
    assertEquals(defn, merged);
  }

  @Test
  public void testMergeAttribs()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", DatasourceSpec.JSON_TYPE);
    raw.put("segmentGranularity", "P1D");
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof DatasourceSpec);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify that one attribute is
    // updated.
    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    assertEquals(((DatasourceSpec) update).segmentGranularity(), ((DatasourceSpec) merged).segmentGranularity());
  }

  @Test
  public void testMergeTags()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", DatasourceSpec.JSON_TYPE);
    raw.put("tags", ImmutableMap.of("tag1", "updated", "tag3", "third value"));
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof DatasourceSpec);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify that one attribute is
    // updated.
    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    Map<String, Object> tags = merged.tags();
    assertEquals(3, tags.size());
    assertEquals("updated", tags.get("tag1"));
    assertEquals("second value", tags.get("tag2"));
    assertEquals("third value", tags.get("tag3"));
  }

  @Test
  public void testMergeHiddenCols()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", DatasourceSpec.JSON_TYPE);
    raw.put("hiddenColumns", Collections.singletonList("mumble"));
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof DatasourceSpec);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify that one attribute is
    // updated.
    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    assertEquals(
        Arrays.asList("foo", "bar", "mumble"),
        ((DatasourceSpec) merged).hiddenColumns()
    );
  }

  @Test
  public void testMergeCols()
  {
    ObjectMapper mapper = new ObjectMapper();
    DatasourceSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", DatasourceSpec.JSON_TYPE);
    raw.put("columns", Arrays.asList(
          ImmutableMap.of(
              "type", DimensionSpec.JSON_TYPE,
              "name", "a",
              "sqlType", "BIGINT",
              "tags", ImmutableMap.of("tag", "value")
          ),
          ImmutableMap.of(
              "type", DimensionSpec.JSON_TYPE,
              "name", "c",
              "sqlType", "VARCHAR"
          )
        )
    );
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof DatasourceSpec);

    // We know that an empty map will leave the spec unchanged
    // due to testMergeEmpty. Here we verify that one attribute is
    // updated.
    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    List<DatasourceColumnSpec> columns = ((DatasourceSpec) merged).columns();
    assertEquals(3, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals("BIGINT", columns.get(0).sqlType());
    assertEquals(1, columns.get(0).tags().size());

    assertEquals("c", columns.get(2).name());
    assertEquals("VARCHAR", columns.get(2).sqlType());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.forClass(DatasourceSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
