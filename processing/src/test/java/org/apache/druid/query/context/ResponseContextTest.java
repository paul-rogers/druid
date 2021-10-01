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

package org.apache.druid.query.context;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.ImmutableMap;
import com.google.common.base.Strings;
import org.apache.druid.jackson.DefaultObjectMapper;
import org.apache.druid.java.util.common.Intervals;
import org.apache.druid.java.util.common.NonnullPair;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext.Key;
import org.apache.druid.query.context.ResponseContext.Keys;
import org.apache.druid.query.context.ResponseContext.StringKey;
import org.apache.druid.query.context.ResponseContext.Visibility;
import org.apache.druid.query.context.ResponseContext.CounterKey;
import org.joda.time.Interval;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResponseContextTest
{
  // Droppable header key
  static final Key EXTN_STRING_KEY = new StringKey(
      "extn_string_key", Visibility.HEADER_AND_TRAILER, true);
  // Non-droppable header key
  static final Key EXTN_COUNTER_KEY = new CounterKey(
      "extn_counter_key", Visibility.HEADER_AND_TRAILER);

  static {
    Keys.instance().registerKeys(new Key[] {
        EXTN_STRING_KEY,
        EXTN_COUNTER_KEY
    });
  }
  
  static final Key UNREGISTERED_KEY = new StringKey(
      "unregistered-key", Visibility.HEADER_AND_TRAILER, true);

  @Test(expected = IllegalStateException.class)
  public void putISETest()
  {
    ResponseContext.createEmpty().put(UNREGISTERED_KEY, new Object());
  }

  @Test(expected = IllegalStateException.class)
  public void addISETest()
  {
    ResponseContext.createEmpty().add(UNREGISTERED_KEY, new Object());
  }

  @Test(expected = IllegalArgumentException.class)
  public void registerKeyIAETest()
  {
    Keys.instance.registerKey(Keys.NUM_SCANNED_ROWS);
  }

  @Test
  public void mergeValueTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.add(Keys.ETAG, "dummy-etag");
    Assert.assertEquals("dummy-etag", ctx.get(Keys.ETAG));
    ctx.add(Keys.ETAG, "new-dummy-etag");
    Assert.assertEquals("new-dummy-etag", ctx.get(Keys.ETAG));

    final Interval interval01 = Intervals.of("2019-01-01/P1D");
    ctx.add(Keys.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    Assert.assertArrayEquals(
        Collections.singletonList(interval01).toArray(),
        ((List<?>) ctx.get(Keys.UNCOVERED_INTERVALS)).toArray()
    );
    final Interval interval12 = Intervals.of("2019-01-02/P1D");
    final Interval interval23 = Intervals.of("2019-01-03/P1D");
    ctx.add(Keys.UNCOVERED_INTERVALS, Arrays.asList(interval12, interval23));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12, interval23).toArray(),
        ((List<?>) ctx.get(Keys.UNCOVERED_INTERVALS)).toArray()
    );

    final String queryId = "queryId";
    final String queryId2 = "queryId2";
    ctx.put(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new ConcurrentHashMap<>());
    ctx.add(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new NonnullPair<>(queryId, 3));
    ctx.add(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new NonnullPair<>(queryId2, 4));
    ctx.add(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new NonnullPair<>(queryId, -1));
    ctx.add(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS, new NonnullPair<>(queryId, -2));
    Assert.assertEquals(
        ImmutableMap.of(queryId, 0, queryId2, 4),
        ctx.get(Keys.REMAINING_RESPONSES_FROM_QUERY_SERVERS)
    );

    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx.add(Keys.MISSING_SEGMENTS, Collections.singletonList(sd01));
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List<?>) ctx.get(Keys.MISSING_SEGMENTS)).toArray()
    );
    final SegmentDescriptor sd12 = new SegmentDescriptor(interval12, "12", 1);
    final SegmentDescriptor sd23 = new SegmentDescriptor(interval23, "23", 2);
    ctx.add(Keys.MISSING_SEGMENTS, Arrays.asList(sd12, sd23));
    Assert.assertArrayEquals(
        Arrays.asList(sd01, sd12, sd23).toArray(),
        ((List<?>) ctx.get(Keys.MISSING_SEGMENTS)).toArray()
    );

    ctx.add(Keys.NUM_SCANNED_ROWS, 0L);
    Assert.assertEquals(0L, ctx.get(Keys.NUM_SCANNED_ROWS));
    ctx.add(Keys.NUM_SCANNED_ROWS, 1L);
    Assert.assertEquals(1L, ctx.get(Keys.NUM_SCANNED_ROWS));
    ctx.add(Keys.NUM_SCANNED_ROWS, 3L);
    Assert.assertEquals(4L, ctx.get(Keys.NUM_SCANNED_ROWS));

    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(false, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, true);
    Assert.assertEquals(true, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
    ctx.add(Keys.UNCOVERED_INTERVALS_OVERFLOWED, false);
    Assert.assertEquals(true, ctx.get(Keys.UNCOVERED_INTERVALS_OVERFLOWED));
  }

  @Test
  public void mergeResponseContextTest()
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.put(Keys.ETAG, "dummy-etag-1");
    final Interval interval01 = Intervals.of("2019-01-01/P1D");
    ctx1.put(Keys.UNCOVERED_INTERVALS, Collections.singletonList(interval01));
    ctx1.put(Keys.NUM_SCANNED_ROWS, 1L);

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    ctx2.put(Keys.ETAG, "dummy-etag-2");
    final Interval interval12 = Intervals.of("2019-01-02/P1D");
    ctx2.put(Keys.UNCOVERED_INTERVALS, Collections.singletonList(interval12));
    final SegmentDescriptor sd01 = new SegmentDescriptor(interval01, "01", 0);
    ctx2.put(Keys.MISSING_SEGMENTS, Collections.singletonList(sd01));
    ctx2.put(Keys.NUM_SCANNED_ROWS, 2L);

    ctx1.merge(ctx2);
    Assert.assertEquals("dummy-etag-2", ctx1.get(Keys.ETAG));
    Assert.assertEquals(3L, ctx1.get(Keys.NUM_SCANNED_ROWS));
    Assert.assertArrayEquals(
        Arrays.asList(interval01, interval12).toArray(),
        ((List<?>) ctx1.get(Keys.UNCOVERED_INTERVALS)).toArray()
    );
    Assert.assertArrayEquals(
        Collections.singletonList(sd01).toArray(),
        ((List<?>) ctx1.get(Keys.MISSING_SEGMENTS)).toArray()
    );
  }

  @Test(expected = IllegalStateException.class)
  public void mergeISETest()
  {
    final ResponseContext ctx = new ResponseContext()
    {
      @Override
      protected Map<Key, Object> getDelegate()
      {
        return ImmutableMap.of(UNREGISTERED_KEY, "non-registered-key");
      }
    };
    ResponseContext.createEmpty().merge(ctx);
  }

  @Test
  public void serializeWithCorrectnessTest() throws JsonProcessingException
  {
    final ResponseContext ctx1 = ResponseContext.createEmpty();
    ctx1.add(EXTN_STRING_KEY, "string-value");
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of(
        		EXTN_STRING_KEY.getName(), 
        		"string-value")),
        ctx1.toHeader(mapper, Integer.MAX_VALUE).getResult()
    );

    final ResponseContext ctx2 = ResponseContext.createEmpty();
    // Add two non-header fields, and one that will be in the header
    ctx2.add(Keys.ETAG, "not in header");
    ctx2.add(Keys.CPU_CONSUMED_NANOS, 100);
    ctx2.add(EXTN_COUNTER_KEY, 100);
    Assert.assertEquals(
        mapper.writeValueAsString(ImmutableMap.of(
        		EXTN_COUNTER_KEY.getName(), 100)),
        ctx2.toHeader(mapper, Integer.MAX_VALUE).getResult()
    );
  }

  @Test
  public void serializeWithTruncateValueTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(EXTN_COUNTER_KEY, 100L);
    ctx.put(EXTN_STRING_KEY, "long-string-that-is-supposed-to-be-removed-from-result");
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final String fullString = objectMapper.writeValueAsString(ctx.getDelegate());
    final ResponseContext.SerializationResult res1 = ctx.toHeader(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(fullString, res1.getResult());
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    ctxCopy.merge(ctx);
    final int target = EXTN_COUNTER_KEY.getName().length() + 3 +
                       Keys.TRUNCATED.getName().length() + 5 +
                       15; // Fudge factor for quotes, separators, etc.
    final ResponseContext.SerializationResult res2 = ctx.toHeader(objectMapper, target);
    ctxCopy.remove(EXTN_STRING_KEY);
    ctxCopy.put(Keys.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        ResponseContext.deserialize(res2.getResult(), objectMapper).getDelegate()
    );
  }

  // Interval value for the test. Must match the deserialized value.
  private static Interval interval(int n) {
    return Intervals.of(String.format("2021-01-%02d/PT1M", n));
  }
  
  // Length of above with quotes and comma.
  static private final int INTERVAL_LEN = 52; 
  
  @Test
  public void serializeWithTruncateArrayTest() throws IOException
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.put(
        Keys.UNCOVERED_INTERVALS,
        Arrays.asList(interval(1), interval(2), interval(3), interval(4),
                      interval(5), interval(6))
    );
    // This value should be longer than the above so it is fully removed
    // before we truncate the above.
    ctx.put(
        EXTN_STRING_KEY,
        Strings.repeat("x", INTERVAL_LEN * 7)
    );
    final DefaultObjectMapper objectMapper = new DefaultObjectMapper();
    final String fullString = objectMapper.writeValueAsString(ctx.getDelegate());
    final ResponseContext.SerializationResult res1 = ctx.toHeader(objectMapper, Integer.MAX_VALUE);
    Assert.assertEquals(fullString, res1.getResult());
    final int maxLen = INTERVAL_LEN * 4 + Keys.UNCOVERED_INTERVALS.getName().length() + 4 +
                       Keys.TRUNCATED.getName().length() + 6;
    final ResponseContext.SerializationResult res2 = ctx.toHeader(objectMapper, maxLen);
    final ResponseContext ctxCopy = ResponseContext.createEmpty();
    // The resulting key array length will be half the start
    // length.
    ctxCopy.put(Keys.UNCOVERED_INTERVALS, Arrays.asList(interval(1), interval(2), interval(3)));
    ctxCopy.put(Keys.TRUNCATED, true);
    Assert.assertEquals(
        ctxCopy.getDelegate(),
        ResponseContext.deserialize(res2.getResult(), objectMapper).getDelegate()
    );
  }

  @Test
  public void deserializeTest() throws IOException
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    final ResponseContext ctx = ResponseContext.deserialize(
        mapper.writeValueAsString(
            ImmutableMap.of(
                Keys.ETAG.getName(), "string-value",
                Keys.NUM_SCANNED_ROWS.getName(), 100L,
                Keys.CPU_CONSUMED_NANOS.getName(), 100000L
            )
        ),
        mapper
    );
    Assert.assertEquals("string-value", ctx.get(Keys.ETAG));
    Assert.assertEquals(100L, ctx.get(Keys.NUM_SCANNED_ROWS));
    Assert.assertEquals(100000L, ctx.get(Keys.CPU_CONSUMED_NANOS));
    ctx.add(Keys.NUM_SCANNED_ROWS, 10L);
    Assert.assertEquals(110L, ctx.get(Keys.NUM_SCANNED_ROWS));
    ctx.add(Keys.CPU_CONSUMED_NANOS, 100L);
    Assert.assertEquals(100100L, ctx.get(Keys.CPU_CONSUMED_NANOS));
  }

  @Test(expected = IllegalStateException.class)
  public void deserializeISETest() throws IOException
  {
    final DefaultObjectMapper mapper = new DefaultObjectMapper();
    ResponseContext.deserialize(
        mapper.writeValueAsString(ImmutableMap.of("ETag_unexpected", "string-value")),
        mapper
    );
  }

  @Test
  public void extensionEnumMergeTest()
  {
    final ResponseContext ctx = ResponseContext.createEmpty();
    ctx.add(Keys.ETAG, "etag");
    ctx.add(EXTN_STRING_KEY, "string-value");
    ctx.add(EXTN_COUNTER_KEY, 2L);
    final ResponseContext ctxFinal = ResponseContext.createEmpty();
    ctxFinal.add(Keys.ETAG, "old-etag");
    ctxFinal.add(EXTN_STRING_KEY, "old-string-value");
    ctxFinal.add(EXTN_COUNTER_KEY, 1L);
    ctxFinal.merge(ctx);
    Assert.assertEquals("etag", ctxFinal.get(Keys.ETAG));
    Assert.assertEquals("string-value", ctxFinal.get(EXTN_STRING_KEY));
    Assert.assertEquals(1L + 2L, ctxFinal.get(EXTN_COUNTER_KEY));
  }
}
