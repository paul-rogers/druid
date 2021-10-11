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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.query.Druids.SegmentMetadataQueryBuilder;
import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.profile.OperatorProfile.OpaqueOperator;
import org.apache.druid.timeline.SegmentId;
import org.junit.Test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Lists;

public class ProfileTest
{
  private static Query<?> mockQuery() {
    return new SegmentMetadataQueryBuilder()
        .dataSource("myTable")
        .build();
  }
  
  @SuppressWarnings("unlikely-arg-type")
  @Test
  public void testFragment() throws JsonProcessingException
  {
    FragmentProfile fragment = new RootNativeFragmentProfile(
        "myHost", "myService", "from-addr",
        "myQuery",  mockQuery(),
        Lists.newArrayList("foo", "bar"),
        123456, 789, 123, 456,
        new OpaqueOperator());
    assertFalse(fragment.equals(null));
    assertFalse(fragment.equals("foo"));
    assertTrue(fragment.equals(fragment));
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    String json = mapper.writeValueAsString(fragment);
    ChildFragmentProfile newValue = mapper.readerFor(ChildFragmentProfile.class).readValue(json);
    assertTrue(fragment.equals(newValue));
  }
  
  /**
   * Test that the fragment profile goes into the response context as
   * a fragment profile object, but is deserialized as a map.
   * @throws IOException 
   */
  @Test
  public void testResponseContext() throws IOException {
    ResponseContext ctx = ResponseContext.createEmpty();
    FragmentProfile fragment = new RootNativeFragmentProfile(
        "myHost", "myService", "from-addr",
        "myQuery", mockQuery(),
        Lists.newArrayList("foo", "bar"),
        123456, 789, 123, 456,
        new OpaqueOperator());
    ctx.put(ResponseContext.Keys.PROFILE, fragment);
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    String json = mapper.writeValueAsString(ctx.trailerCopy());
    ResponseContext newValue = ResponseContext.deserialize(json, mapper);
    
    // Verify that the profile was decoded as a map (to ensure compatibility).
    @SuppressWarnings("unchecked")
    Map<String, Object> map = (Map<String, Object>) newValue.get(ResponseContext.Keys.PROFILE);
    assertEquals("myQuery", map.get("queryId"));
    @SuppressWarnings("unchecked")
    Map<String, Object> queryMap = (Map<String, Object>) map.get("query");
    @SuppressWarnings("unchecked")
    Map<String, Object> dsMap = (Map<String, Object>) queryMap.get("dataSource");
    assertEquals("myTable", dsMap.get("name"));
  }
  
  @Test
  public void testOperatorStack() {
    ResponseContext ctx = ResponseContext.createEmpty();
    OperatorProfile op = ctx.popProfile();
    assertNotNull(op);
    assertTrue(op instanceof OpaqueOperator);
    
    SortProfile sort1 = new SortProfile();
    sort1.timeNs = 1;
    ctx.pushProfile(sort1);
    SortProfile sort2 = new SortProfile();
    sort2.timeNs = 2;
    ctx.pushProfile(sort2);
    assertSame(sort2, ctx.popProfile());
    assertSame(sort1, ctx.popProfile());
    assertTrue(ctx.popProfile() instanceof OpaqueOperator);
    
    ctx.pushGroup();
    ctx.pushProfile(sort1);
    ctx.pushProfile(sort2);
    assertEquals(2, ctx.popGroup().size());
    assertTrue(ctx.popProfile() instanceof OpaqueOperator);
  }
  
  @Test(expected = IllegalStateException.class)
  public void testGroupUnderflow() {
    ResponseContext ctx = ResponseContext.createEmpty();
    ctx.popGroup();
  }
  
  @Test
  public void testSerializeOperators() throws JsonProcessingException {
    ConcatProfile concat = new ConcatProfile();
    concat.timeNs = 1;
    
    OpaqueOperator opaque = new OpaqueOperator();
    opaque.timeNs = 2;
    
    ReceiverProfile receiver = new ReceiverProfile(
        "foo:123",
        "aUrl");
    receiver.timeNs = 3;
    receiver.succeeded = true;
    receiver.error = "An error";
    receiver.firstByteNs = 100;
    receiver.backPressureNs = 101;
    receiver.rows = 102;
    receiver.bytes = 103;
    receiver.response = new HashMap<>();
    receiver.response.put("foo", "bar");
    receiver.fragment = new HashMap<>();
    receiver.fragment.put("fred", "wilma");
    
    SegmentMetadataScanProfile segmentMD = new SegmentMetadataScanProfile(
        SegmentId.dummy("data-source", 3));
    segmentMD.timeNs = 4;
    
    MergeProfile merge = new MergeProfile();
    merge.timeNs = 5;
    merge.children = Lists.newArrayList(new OpaqueOperator());
    
    SortProfile sort = new SortProfile();
    sort.timeNs = 6;
    sort.child = new OpaqueOperator();
    
    SegmentScanProfile segScan = new SegmentScanProfile(
        SegmentId.dummy("data-source", 3));
    segScan.timeNs = 7;
    segScan.rows = 100;
    segScan.limited = true;
    segScan.columnCount = 101;
    segScan.batchSize = 102;
    segScan.cursorCount = 103;
    segScan.error = "An error";
    
    ConcatProfile container = new ConcatProfile();
    container.children = Lists.newArrayList(
        opaque,
        concat,
        receiver,
        segmentMD,
        merge,
        sort,
        segScan);
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    String json = mapper.writeValueAsString(container);
    System.out.println(json);
  }
}
