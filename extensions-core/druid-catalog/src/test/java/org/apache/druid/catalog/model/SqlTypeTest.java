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

package org.apache.druid.catalog.model;

import org.apache.druid.java.util.common.IAE;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SqlTypeTest
{
  @Test
  public void testVarchar()
  {
    assertEquals("VARCHAR", PropertyConverter.VARCHAR_TYPE.sqlJavaType());
    assertEquals("foo", PropertyConverter.VARCHAR_TYPE.convert("foo"));
    try {
      PropertyConverter.VARCHAR_TYPE.convert(10);
      fail();
    }
    catch (IAE e) {
      // Expected;
    }
  }

  @Test
  public void testVarcharList()
  {
    assertEquals("VARCHAR", PropertyConverter.VARCHAR_LIST_TYPE.sqlJavaType());
    assertEquals(Collections.singletonList("foo"), PropertyConverter.VARCHAR_LIST_TYPE.convert("foo"));
    assertEquals(Arrays.asList("foo", "bar"), PropertyConverter.VARCHAR_LIST_TYPE.convert("foo,bar"));
    assertEquals(Arrays.asList("foo", "bar"), PropertyConverter.VARCHAR_LIST_TYPE.convert("foo,  bar"));
    try {
      PropertyConverter.VARCHAR_LIST_TYPE.convert(10);
      fail();
    }
    catch (IAE e) {
      // Expected;
    }
  }

  @Test
  public void testVarcharFileList()
  {
    assertEquals("VARCHAR", PropertyConverter.VARCHAR_FILE_LIST_TYPE.sqlJavaType());
    assertEquals(
        Collections.singletonList(new File("foo")),
        PropertyConverter.VARCHAR_FILE_LIST_TYPE.convert("foo"));
    assertEquals(
        Arrays.asList(new File("foo"), new File("bar")),
        PropertyConverter.VARCHAR_FILE_LIST_TYPE.convert("foo,bar"));
    assertEquals(
        Arrays.asList(new File("foo"), new File("bar")),
        PropertyConverter.VARCHAR_FILE_LIST_TYPE.convert("foo,  bar"));
    try {
      PropertyConverter.VARCHAR_FILE_LIST_TYPE.convert(10);
      fail();
    }
    catch (IAE e) {
      // Expected;
    }
  }

  @Test
  public void testBoolean()
  {
    assertEquals("BOOLEAN", PropertyConverter.BOOLEAN_TYPE.sqlJavaType());
    assertTrue(PropertyConverter.BOOLEAN_TYPE.convert("true"));
    assertTrue(PropertyConverter.BOOLEAN_TYPE.convert(true));
    assertFalse(PropertyConverter.BOOLEAN_TYPE.convert("false"));
    assertFalse(PropertyConverter.BOOLEAN_TYPE.convert(false));
    try {
      PropertyConverter.BOOLEAN_TYPE.convert(10);
      fail();
    }
    catch (IAE e) {
      // Expected;
    }
  }

  @Test
  public void testInt()
  {
    assertEquals("INT", PropertyConverter.INT_TYPE.sqlJavaType());
    assertEquals(0, (int) PropertyConverter.INT_TYPE.convert(0));
    assertEquals(0, (int) PropertyConverter.INT_TYPE.convert("0"));
    assertEquals(10, (int) PropertyConverter.INT_TYPE.convert(10));
    assertEquals(10, (int) PropertyConverter.INT_TYPE.convert("10"));
    try {
      PropertyConverter.INT_TYPE.convert("foo");
      fail();
    }
    catch (IAE e) {
      // Expected;
    }
  }
}
