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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class SubTypeTest
{
  @Test
  public void testPropTypes()
  {
    PropertyConverter[] props = {
        new PropertyConverter("v", "jv", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("vl", "jvl", PropertyConverter.VARCHAR_LIST_TYPE),
        new PropertyConverter("vfl", "jvfl", PropertyConverter.VARCHAR_FILE_LIST_TYPE),
    };
    SubTypeConverter converter = new SubTypeConverter("st", "type", props);

    Map<String, Object> args = ImmutableMap.of("v", "foo", "vl", "a, b", "vfl", "f1, f2");
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

    Map<String, Object> expected = ImmutableMap.of(
        "jv",
        "foo",
        "jvl",
        Arrays.asList("a", "b"),
        "jvfl",
        Arrays.asList(new File("f1"), new File("f2")));
    assertEquals(expected, jsonMap);

    Map<String, PropertyConverter> propMap = new HashMap<>();
    converter.gatherProperties(propMap);
    assertEquals(3, propMap.size());
    assertSame(props[0], propMap.get("v"));
  }

  @Test
  public void testDuplicateProps()
  {
    PropertyConverter[] props = {
        new PropertyConverter("foo", "foo", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("foo", "foo", PropertyConverter.VARCHAR_TYPE),
    };
    assertThrows(ISE.class, () -> new SubTypeConverter("st", "type", props));
  }

  @Test
  public void testWrongType()
  {
    PropertyConverter[] props = {
        new PropertyConverter("v", "jv", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter converter = new SubTypeConverter("st", "type", props);

    Map<String, Object> args = ImmutableMap.of("v", 10);
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    assertThrows(IAE.class, () -> converter.convert(modelArgs, Collections.emptyList()));
  }

  @Test
  public void testNoMatch()
  {
    PropertyConverter[] props = {
        new PropertyConverter("v", "jv", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter converter = new SubTypeConverter("st", "type", props);

    Map<String, Object> args = ImmutableMap.of("x", "bar");
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    assertTrue(jsonMap.isEmpty());
    assertFalse(modelArgs.get("x").isConsumed());
  }

  @Test
  public void testPartialMatch()
  {
    PropertyConverter[] props = {
        new PropertyConverter("v", "jv", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter converter = new SubTypeConverter("st", "type", props);

    Map<String, Object> args = ImmutableMap.of("v", "foo", "x", "bar");
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    assertEquals(1, jsonMap.size());
    assertEquals("foo", jsonMap.get("jv"));
    assertTrue(modelArgs.get("v").isConsumed());
    assertFalse(modelArgs.get("x").isConsumed());
  }

  @Test
  public void testOverloadCollision()
  {
    PropertyConverter[] props = {
        new PropertyConverter("v", "jv", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("alias", "jv", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter converter = new SubTypeConverter("st", "type", props);

    Map<String, Object> args = ImmutableMap.of("v", "foo", "alias", "bar");
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    assertThrows(IAE.class, () -> converter.convert(modelArgs, Collections.emptyList()));
  }
}
