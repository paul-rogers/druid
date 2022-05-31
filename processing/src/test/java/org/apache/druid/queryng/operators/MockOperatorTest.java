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

package org.apache.druid.queryng.operators;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceTestHelper;
import org.apache.druid.queryng.fragment.FragmentBuilder;
import org.apache.druid.queryng.fragment.FragmentBuilder.ResultIterator;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Use a mock operator to test (and illustrate) the basic operator
 * mechanisms.
 */
public class MockOperatorTest
{
  @Test
  public void testMockStringOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<String> op = MockOperator.strings(builder, 2);
    ResultIterator<String> iter = builder.open();
    assertTrue(iter.hasNext());
    assertEquals("Mock row 0", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("Mock row 1", iter.next());
    assertFalse(iter.hasNext());
    iter.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testMockIntOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 2);
    ResultIterator<Integer> iter = builder.open();
    assertTrue(iter.hasNext());
    assertEquals(0, (int) iter.next());
    assertTrue(iter.hasNext());
    assertEquals(1, (int) iter.next());
    assertFalse(iter.hasNext());
    iter.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testIterator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 2);
    int rid = 0;
    ResultIterator<Integer> iter = builder.open();
    for (Integer row : Operators.toIterable(iter)) {
      assertEquals(rid++, (int) row);
    }
    iter.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testToList() throws IOException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 5);
    final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4);
    final List<Integer> actual = builder.toList(op);
    assertEquals(expected, actual);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  // An operator is a one-pass object, don't try sequence tests that assume
  // the sequence is reentrant.
  @Test
  public void testSequenceYielder() throws IOException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 5);
    final List<Integer> expected = Arrays.asList(0, 1, 2, 3, 4);
    Sequence<Integer> seq = builder.toSequence(op);
    SequenceTestHelper.testYield("op", 5, seq, expected);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  @Test
  public void testSequenceAccum() throws IOException
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 4);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
    Sequence<Integer> seq = builder.toSequence(op);
    SequenceTestHelper.testAccumulation("op", seq, vals);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Example of how the fragment context ensures dynamically-created
   * operators are closed.
   */
  @Test
  public void testRootOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    MockOperator<Integer> op = MockOperator.ints(builder, 2);
    ResultIterator<Integer> iter = builder.open(op);
    assertTrue(iter.hasNext());
    assertEquals(0, (int) iter.next());
    assertTrue(iter.hasNext());
    assertEquals(1, (int) iter.next());
    assertFalse(iter.hasNext());
    iter.close();
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   */
  @Test
  public void testSequenceOperator()
  {
    FragmentBuilder builder = FragmentBuilder.defaultBuilder();
    Operator<String> op = MockOperator.strings(builder, 2);
    Sequence<String> seq = Operators.toSequence(op);
    Operator<String> outer = Operators.toOperator(builder, seq);
    ResultIterator<String> iter = builder.open(outer);
    assertTrue(iter.hasNext());
    assertEquals("Mock row 0", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("Mock row 1", iter.next());
    assertFalse(iter.hasNext());
    iter.close();
  }
}
