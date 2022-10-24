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

package org.apache.druid.exec.util;

import org.apache.druid.exec.util.TypeRegistry.TypeAttributes;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class TypeRegistryTest
{
  @Test
  public void testTypes()
  {
    TypeRegistry reg = TypeRegistry.INSTANCE;

    TypeAttributes attribs = reg.resolve(ColumnType.STRING);
    assertEquals(ColumnType.STRING, attribs.type());
    assertTrue(attribs.comperable());
    assertNotNull(attribs.objectOrdering());

    attribs = reg.resolve(ColumnType.LONG);
    assertEquals(ColumnType.LONG, attribs.type());
    assertTrue(attribs.comperable());
    assertNotNull(attribs.objectOrdering());

    attribs = reg.resolve(ColumnType.FLOAT);
    assertEquals(ColumnType.FLOAT, attribs.type());
    assertTrue(attribs.comperable());
    assertNotNull(attribs.objectOrdering());

    attribs = reg.resolve(ColumnType.DOUBLE);
    assertEquals(ColumnType.DOUBLE, attribs.type());
    assertTrue(attribs.comperable());
    assertNotNull(attribs.objectOrdering());

    attribs = reg.resolve(ColumnType.UNKNOWN_COMPLEX);
    assertEquals(ColumnType.UNKNOWN_COMPLEX, attribs.type());
    assertFalse(attribs.comperable());
    assertNull(attribs.objectOrdering());

    // Non-scalars not yet supported
    attribs = reg.resolve(ColumnType.STRING_ARRAY);
    assertNull(attribs);
  }

  @Test
  public void testComparators()
  {
    TypeRegistry reg = TypeRegistry.INSTANCE;

    // Error cases.
    final SortColumn key = new SortColumn("foo", false);
    assertThrows(ISE.class, () -> reg.sortOrdering(key, null));
    assertThrows(ISE.class, () -> reg.sortOrdering(key, ColumnType.STRING_ARRAY));
    assertThrows(ISE.class, () -> reg.sortOrdering(key, ColumnType.UNKNOWN_COMPLEX));

    // String
    Comparator<Object> comp = reg.sortOrdering(key, ColumnType.STRING);
    assertTrue(comp.compare("a", "b") < 0);
    assertTrue(comp.compare("a", "a") == 0);
    assertTrue(comp.compare("b", "a") > 0);

    // Long
    comp = reg.sortOrdering(key, ColumnType.STRING);
    assertTrue(comp.compare(1L, 2L) < 0);
    assertTrue(comp.compare(1L, 1L) == 0);
    assertTrue(comp.compare(2L, 1L) > 0);

    // Float
    reg.sortOrdering(key, ColumnType.STRING);
    assertTrue(comp.compare(1f, 2f) < 0);
    assertTrue(comp.compare(1f, 1f) == 0);
    assertTrue(comp.compare(2f, 1f) > 0);

    // Since floats are sometimes stored as doubles
    assertTrue(comp.compare(1d, 2d) < 0);
    assertTrue(comp.compare(1d, 1d) == 0);
    assertTrue(comp.compare(2d, 1d) > 0);

    // Double
    comp = reg.sortOrdering(key, ColumnType.STRING);
    assertTrue(comp.compare(1d, 2d) < 0);
    assertTrue(comp.compare(1d, 1d) == 0);
    assertTrue(comp.compare(2d, 1d) > 0);

    // Descending
    final SortColumn key2 = new SortColumn("foo", true);
    comp = reg.sortOrdering(key2, ColumnType.STRING);
    assertTrue(comp.compare("a", "b") > 0);
    assertTrue(comp.compare("a", "a") == 0);
    assertTrue(comp.compare("b", "a") < 0);
  }
}
