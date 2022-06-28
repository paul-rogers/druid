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

import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

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
        "a\nc\n");
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
    InputTableSpec spec = InputTableSpec.builder().build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
    }

    Map<String, Object> props = ImmutableMap.of(
        "source",
        "inline",
        "format",
        "csv",
        "data",
        "a,b\nc,d\n");
    spec = InputTableSpec
        .builder()
        .properties(props)
        .column("a", "varchar")
        .column("a", "varchar")
        .build();
    try {
      spec.validate();
      fail();
    }
    catch (IAE e) {
      // Expected
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
        "a\nc\n");
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

  @Test
  public void testEquals()
  {
    EqualsVerifier.simple()
                  .forClass(InputTableSpec.class)
                  .usingGetClass()
                  .verify();
  }
}
