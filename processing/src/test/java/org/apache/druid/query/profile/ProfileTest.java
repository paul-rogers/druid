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

package org.apache.druid.query.profile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;

public class ProfileTest
{
  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void testFragment() throws JsonProcessingException
  {
    FragmentNode fragment = new FragmentNode(
        "myHost", "from-addr",
        123456, 789, 123);
    assertFalse(fragment.equals(null));
    assertFalse(fragment.equals("foo"));
    assertTrue(fragment.equals(fragment));
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    String json = mapper.writeValueAsString(fragment);
    FragmentNode newValue = mapper.readerFor(FragmentNode.class).readValue(json);
    assertTrue(fragment.equals(newValue));
  }
  
  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void testSlice() throws JsonProcessingException
  {
    FragmentNode fragment1 = new FragmentNode(
        "myHost", "from-addr",
        123456, 789, 123);
    FragmentNode fragment2 = new FragmentNode(
        "host2", "from-addr",
        1234560, 7890, 1230);
    assertFalse(fragment1.equals(fragment2));
    
    SliceNode slice1 = new SliceNode("query1");
    assertEquals(0, slice1.getFragments().size());
    slice1.add(fragment1);
    assertFalse(slice1.equals(null));
    assertFalse(slice1.equals("foo"));
    assertTrue(slice1.equals(slice1));
    
    SliceNode slice2 = new SliceNode("query1");
    assertFalse(slice1.equals(slice2));
    slice2.add(fragment2);
    assertFalse(slice1.equals(slice2));
    slice1.merge(slice2);
    assertEquals(2, slice1.getFragments().size());
    
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    String json = mapper.writeValueAsString(slice1);
    SliceNode newValue = mapper.readerFor(SliceNode.class).readValue(json);
    assertTrue(slice1.equals(newValue));
    
    SliceNode slice3 = new SliceNode(null);
    assertEquals("anonymous", slice3.getQueryId());
  }
}
