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

package org.apache.druid.queryng.operators.general;

import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.general.MockStorageAdapter.MockSegment;
import org.apache.druid.segment.SegmentReference;
import org.junit.Test;

import java.io.Closeable;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentLockOperatorTest
{
  private static class MockReference extends MockSegment implements SegmentReference, Closeable
  {
    boolean isLocked;
    boolean wasLocked;

    public MockReference()
    {
      super(10);
    }

    @Override
    public Optional<Closeable> acquireReferences()
    {
      if (!isLocked) {
        isLocked = true;
        wasLocked = true;
      }
      return Optional.of(this);
    }

    @Override
    public void close()
    {
      isLocked = false;
    }
  }

  private static class MockMissingSegment extends MockReference
  {
    @Override
    public Optional<Closeable> acquireReferences()
    {
      return Optional.empty();
    }
  }

  @Test
  public void testLock()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockReference segment = new MockReference();
    Operator<Object> op = new SegmentLockOperator<>(
        builder.context(),
        segment,
        MockStorageAdapter.MOCK_DESCRIPTOR,
        new NullOperator<Object>(builder.context())
    );
    List<Object> results = builder.run(op).toList();
    assertTrue(results.isEmpty());
    assertFalse(segment.isLocked);
    assertTrue(segment.wasLocked);
  }

  @Test
  public void testMissingSegment()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockReference segment = new MockMissingSegment();
    Operator<Object> op = new SegmentLockOperator<>(
        builder.context(),
        segment,
        MockStorageAdapter.MOCK_DESCRIPTOR,
        new NullOperator<Object>(builder.context())
    );
    List<Object> results = builder.run(op).toList();
    assertTrue(results.isEmpty());
    assertFalse(segment.isLocked);
    assertFalse(segment.wasLocked);
    assertFalse(builder.context().responseContext().getMissingSegments().isEmpty());
    assertEquals(
        MockStorageAdapter.MOCK_DESCRIPTOR,
        builder.context().responseContext().getMissingSegments().get(0)
    );
  }
}
