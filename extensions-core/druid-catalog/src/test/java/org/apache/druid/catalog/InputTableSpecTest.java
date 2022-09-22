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
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class InputTableSpecTest
{
  @Test
  public void testMinimalBuilder()
  {
    // Minimum possible definition
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a\nc\n"
    );
    InputTableSpec spec = InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .build();

    spec.validate();
    assertEquals(props, spec.properties());
    List<InputColumnSpec> columns = spec.columns();
    assertEquals(1, columns.size());
    assertEquals("a", columns.get(0).name());
    assertEquals("varchar", columns.get(0).sqlType());

    InputTableSpec copy = spec.toBuilder().build();
    assertEquals(spec, copy);
  }

  @Test
  public void testValidation()
  {
    {
      final InputTableSpec spec = InputTableSpec.builder().build();
      assertThrows(IAE.class, () -> spec.validate());
    }

    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a,b\nc,d\n");

    {
      // Happy path
      final InputTableSpec spec = InputTableSpec
          .builder()
          .properties(props)
          .column("a", "varchar")
          .column("b", "varchar")
          .build();
      spec.validate();
    }
    {
      // Fails: duplicate columns
      final InputTableSpec spec = InputTableSpec
          .builder()
          .properties(props)
          .column("a", "varchar")
          .column("a", "varchar")
          .build();
      assertThrows(IAE.class, () -> spec.validate());
    }
  }

  @Test
  public void testSerialization()
  {
    ObjectMapper mapper = new ObjectMapper();
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a\nc\n"
    );
    InputTableSpec spec1 = InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .build();

    // Round-trip
    TableSpec spec2 = TableSpec.fromBytes(mapper, spec1.toBytes(mapper));
    assertEquals(spec1, spec2);

    // Sanity check of toString, which uses JSON
    assertNotNull(spec1.toString());
  }

  private InputTableSpec exampleSpec()
  {
    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a,b\nc,d\n");
    return InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .column("b", "varchar")
        .tag("tag1", "some value")
        .tag("tag2", "second value")
        .build();
  }

  @Test
  public void testMergeEmpty()
  {
    ObjectMapper mapper = new ObjectMapper();
    InputTableSpec defn = exampleSpec();

    // Create an update that mirrors what REST will do.
    Map<String, Object> raw = new HashMap<>();
    raw.put("type", InputTableSpec.JSON_TYPE);
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof InputTableSpec);

    TableSpec merged = defn.merge(update, raw, mapper);
    assertEquals(defn, merged);
  }

  @Test
  public void testMergeTags()
  {
    ObjectMapper mapper = new ObjectMapper();
    InputTableSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", InputTableSpec.JSON_TYPE);
    raw.put("tags", ImmutableMap.of("tag1", "updated", "tag3", "third value"));
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof InputTableSpec);

    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    Map<String, Object> tags = merged.tags();
    assertEquals(3, tags.size());
    assertEquals("updated", tags.get("tag1"));
    assertEquals("second value", tags.get("tag2"));
    assertEquals("third value", tags.get("tag3"));
  }

  @Test
  public void testMergeProperties()
  {
    ObjectMapper mapper = new ObjectMapper();
    InputTableSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", InputTableSpec.JSON_TYPE);
    raw.put("properties", ImmutableMap.of("data", "foo, bar\n"));
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof InputTableSpec);

    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    Map<String, Object> props = ((InputTableSpec) merged).properties();
    assertEquals(3, props.size());
    assertEquals("foo, bar\n", props.get("data"));
  }

  /**
   * For an input source, any provided columns replace (not add to) the
   * existing columns.
   */
  @Test
  public void testMergeColumns()
  {
    ObjectMapper mapper = new ObjectMapper();
    InputTableSpec defn = exampleSpec();

    Map<String, Object> raw = new HashMap<>();
    raw.put("type", InputTableSpec.JSON_TYPE);
    raw.put("columns", Arrays.asList(
            ImmutableMap.of(
                "name", "x",
                "sqlType", "VARCHAR"
            ),
            ImmutableMap.of(
                "name", "y",
                "sqlType", "VARCHAR"
            )
        )
    );
    TableSpec update = mapper.convertValue(raw, TableSpec.class);
    assertTrue(update instanceof InputTableSpec);

    TableSpec merged = defn.merge(update, raw, mapper);
    assertNotEquals(defn, merged);
    assertEquals(((InputTableSpec) update).columns(), ((InputTableSpec) merged).columns());
  }

  @Test
  public void testEquals()
  {
    EqualsVerifier.simple()
                  .forClass(InputTableSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
