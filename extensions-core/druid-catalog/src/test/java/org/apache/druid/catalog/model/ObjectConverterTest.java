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
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ObjectConverterTest
{
  private ObjectConverter buildConverter()
  {
    PropertyConverter[] props1 = {
        new PropertyConverter("a", "ja", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("b", "jb", PropertyConverter.VARCHAR_TYPE),
    };
    PropertyConverter[] props2 = {
        new PropertyConverter("a", "ja", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("c", "jc", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter subType1 = new SubTypeConverter("t1", "type1", props1);
    SubTypeConverter subType2 = new SubTypeConverter("t2", "type2", props2);
    return new ObjectConverter(
        "jk",      // Outer key, not used here
        "myType",  // SQL arg for the type
        "jt",      // JSON key for the type
        new SubTypeConverter[] {subType1, subType2});
  }

  @Test
  public void testProperties()
  {
    ObjectConverter converter = buildConverter();
    Map<String, PropertyConverter> props = new HashMap<>();
    converter.gatherProperties(props);
    assertEquals(4, props.size());
    assertTrue(props.containsKey("myType"));
    assertTrue(props.containsKey("a"));
  }

  @Test
  public void testTypeConflict()
  {
    PropertyConverter[] props1 = {
        new PropertyConverter("a", "ja", PropertyConverter.VARCHAR_TYPE),
    };
    PropertyConverter[] props2 = {
        new PropertyConverter("a", "ja", PropertyConverter.INT_TYPE),
    };
    SubTypeConverter subType1 = new SubTypeConverter("t1", "type1", props1);
    SubTypeConverter subType2 = new SubTypeConverter("t2", "type2", props2);
    ObjectConverter converter = new ObjectConverter(
        "jk",      // Outer key, not used here
        "myType",  // SQL arg for the type
        "jt",      // JSON key for the type
        new SubTypeConverter[] {subType1, subType2});
    try {
      Map<String, PropertyConverter> props = new HashMap<>();
      converter.gatherProperties(props);
      fail();
    }
    catch (ISE e) {
      // Expected
    }
  }

  @Test
  public void testEmpty()
  {
    ObjectConverter converter = buildConverter();
    assertEquals("jk", converter.jsonKey);
    Map<String, Object> jsonMap = converter.convert(Collections.emptyMap(), Collections.emptyList());
    assertNull(jsonMap);
  }

  @Test
  public void testNoMatch()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(ImmutableMap.of("x", "foo"));
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    assertNull(jsonMap);
  }

  @Test
  public void testSubtypeOnly()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(ImmutableMap.of("myType", "t1"));
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    assertEquals(1, jsonMap.size());
    assertEquals("type1", jsonMap.get("jt"));
  }

  @Test
  public void testFullMatchType1()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(
        ImmutableMap.of("myType", "t1", "a", "foo", "b", "bar"));
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    Map<String, Object> expected = ImmutableMap.of(
        "jt",
        "type1",
        "ja",
        "foo",
        "jb",
        "bar");
    assertEquals(expected, jsonMap);
    ModelArg.verifyArgs(modelArgs);
  }

  @Test
  public void testFullMatchType2()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(
        ImmutableMap.of("myType", "t2", "a", "foo", "c", "bar"));
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    Map<String, Object> expected = ImmutableMap.of(
        "jt",
        "type2",
        "ja",
        "foo",
        "jc",
        "bar");
    assertEquals(expected, jsonMap);
    ModelArg.verifyArgs(modelArgs);
  }

  @Test
  public void testPartialMatch()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(
        ImmutableMap.of("myType", "t1", "a", "foo", "x", "bar"));
    Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());
    Map<String, Object> expected = ImmutableMap.of(
        "jt",
        "type1",
        "ja",
        "foo");
    assertEquals(expected, jsonMap);
    assertFalse(modelArgs.get("x").isConsumed());
  }

  @Test
  public void testBadType()
  {
    ObjectConverter converter = buildConverter();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(
        ImmutableMap.of("myType", "t3", "a", "foo", "x", "bar"));
    try {
      converter.convert(modelArgs, Collections.emptyList());
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testDuplicateTypes()
  {
    PropertyConverter[] props1 = {
        new PropertyConverter("a", "ja", PropertyConverter.VARCHAR_TYPE),
    };
    PropertyConverter[] props2 = {
        new PropertyConverter("a", "ja", PropertyConverter.VARCHAR_TYPE),
    };
    SubTypeConverter subType1 = new SubTypeConverter("t1", "type1", props1);
    SubTypeConverter subType2 = new SubTypeConverter("t1", "type2", props2);
    try {
      new ObjectConverter("x", "x", "x", new SubTypeConverter[] {subType1, subType2});
      fail();
    }
    catch (ISE e) {
      // Expected
    }
  }
}
