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
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ModelArgTest
{
  @Test
  public void testConsumed()
  {
    ModelArg arg = new ModelArg("foo", "bar");
    assertEquals("bar", arg.value());
    assertEquals("bar", arg.asString());

    // Not consumed yet
    assertFalse(arg.isConsumed());
    try {
      arg.assertConsumed();
      fail();
    }
    catch (IAE e) {
      // Expected;
    }

    // Consume once
    arg.consume();
    assertTrue(arg.isConsumed());
    arg.assertConsumed();

    // Can't consume again: ambiguous name
    try {
      arg.consume();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testAsString()
  {
    ModelArg arg = new ModelArg("foo", "bar");
    assertEquals("bar", arg.value());
    assertEquals("bar", arg.asString());

    arg = new ModelArg("foo", 10);
    assertEquals(10, arg.value());
    try {
      arg.asString();
      fail();
    }
    catch (IAE e) {
      // Expected
    }
  }

  @Test
  public void testConvert()
  {
    Map<String, Object> args = ImmutableMap.of("a", "foo", "b", 10);
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    assertEquals(2, modelArgs.size());
    assertNotNull(modelArgs.get("a"));
    assertNotNull(modelArgs.get("b"));
  }
}
