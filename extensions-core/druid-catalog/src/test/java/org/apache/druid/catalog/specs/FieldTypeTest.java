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
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.catalog.specs.FieldTypes.FieldTypeDefn;
import org.apache.druid.java.util.common.IAE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class FieldTypeTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testString()
  {
    FieldTypeDefn<String> fieldType = FieldTypes.STRING_TYPE;
    assertEquals("string", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    assertEquals("value", fieldType.decode(mapper, "value"));

    // Jackson is permissive in its conversions
    assertEquals("10", fieldType.decode(mapper, 10));

    // But, it does have its limits.
    assertThrows(Exception.class, () -> fieldType.decode(mapper, Arrays.asList("a", "b")));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertEquals("value", fieldType.decodeSqlValue(mapper, "value"));

    assertNull(fieldType.encodeToJava(null));
    assertEquals("value", fieldType.encodeToJava("value"));
  }

  @Test
  public void testBoolean()
  {
    FieldTypeDefn<Boolean> fieldType = FieldTypes.BOOLEAN_TYPE;
    assertEquals("Boolean", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    assertTrue(fieldType.decode(mapper, "true"));
    assertTrue(fieldType.decode(mapper, true));
    assertFalse(fieldType.decode(mapper, "false"));
    assertFalse(fieldType.decode(mapper, false));
    assertFalse(fieldType.decode(mapper, 0));
    assertTrue(fieldType.decode(mapper, 10));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertTrue(fieldType.decodeSqlValue(mapper, "true"));
    assertTrue(fieldType.decodeSqlValue(mapper, true));
    assertFalse(fieldType.decodeSqlValue(mapper, "false"));
    assertFalse(fieldType.decodeSqlValue(mapper, false));
    assertFalse(fieldType.decodeSqlValue(mapper, 0));
    assertTrue(fieldType.decodeSqlValue(mapper, 10));

    assertNull(fieldType.encodeToJava(null));
    assertEquals(true, fieldType.encodeToJava(true));
    assertEquals(false, fieldType.encodeToJava(false));
  }

  @Test
  public void testInt()
  {
    FieldTypeDefn<Integer> fieldType = FieldTypes.INT_TYPE;
    assertEquals("integer", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    assertEquals((Integer) 0, fieldType.decode(mapper, 0));
    assertEquals((Integer) 0, fieldType.decode(mapper, "0"));
    assertEquals((Integer) 10, fieldType.decode(mapper, 10));
    assertEquals((Integer) 10, fieldType.decode(mapper, "10"));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "foo"));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertEquals((Integer) 0, fieldType.decodeSqlValue(mapper, 0));
    assertEquals((Integer) 0, fieldType.decodeSqlValue(mapper, "0"));
    assertEquals((Integer) 10, fieldType.decodeSqlValue(mapper, 10));
    assertEquals((Integer) 10, fieldType.decodeSqlValue(mapper, "10"));
    assertThrows(Exception.class, () -> fieldType.decodeSqlValue(mapper, "foo"));

    assertNull(fieldType.encodeToJava(null));
    assertEquals((Integer) 10, fieldType.encodeToJava(10));
  }

  @Test
  public void testStringList()
  {
    FieldTypeDefn<List<String>> fieldType = FieldTypes.STRING_LIST_TYPE;
    assertEquals("string list", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    List<String> value = Arrays.asList("a", "b");
    assertEquals(value, fieldType.decode(mapper, value));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "foo"));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "a,b"));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "a, b"));

    assertNull(fieldType.encodeToJava(null));
    assertEquals(value, fieldType.encodeToJava(value));
  }

  @Test
  public void testFileList()
  {
    FieldTypeDefn<List<String>> fieldType = FieldTypes.FILE_LIST_TYPE;
    assertEquals("string list", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    List<String> value = Arrays.asList("a", "b");
    assertEquals(value, fieldType.decode(mapper, value));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "foo"));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "a,b"));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "a, b"));

    assertNull(fieldType.encodeToJava(null));
    List<File> javaValue = Arrays.asList(new File("a"), new File("b"));
    assertEquals(javaValue, fieldType.encodeToJava(value));
  }

  @Test
  public void testUriList() throws URISyntaxException
  {
    FieldTypeDefn<List<String>> fieldType = FieldTypes.URI_LIST_TYPE;
    assertEquals("string list", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    List<String> value = Arrays.asList("http://foo.com", "https://bar.com");
    assertEquals(value, fieldType.decode(mapper, value));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "foo"));

    assertNull(fieldType.decodeSqlValue(mapper, null));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "http://foo.com,https://bar.com"));
    assertEquals(value, fieldType.decodeSqlValue(mapper, "http://foo.com, https://bar.com"));

    assertNull(fieldType.encodeToJava(null));
    List<URI> javaValue = Arrays.asList(new URI("http://foo.com"), new URI("https://bar.com"));
    assertEquals(javaValue, fieldType.encodeToJava(value));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "bogus"));
  }

  @Test
  public void testClusterKeyList()
  {
    FieldTypeDefn<List<ClusterKeySpec>> fieldType = FieldTypes.CLUSTER_KEY_LIST_TYPE;
    assertEquals("cluster key list", fieldType.typeName());

    assertNull(fieldType.decode(mapper, null));
    List<Map<String, Object>> value = Arrays.asList(
        ImmutableMap.of("column", "a"),
        ImmutableMap.of("column", "b", "desc", true)
    );
    List<ClusterKeySpec> expected = Arrays.asList(
        new ClusterKeySpec("a", false),
        new ClusterKeySpec("b", true)
    );
    assertEquals(expected, fieldType.decode(mapper, value));
    assertThrows(Exception.class, () -> fieldType.decode(mapper, "foo"));
  }
}
