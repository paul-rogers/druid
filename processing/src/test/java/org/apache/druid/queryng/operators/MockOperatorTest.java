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
import org.apache.druid.queryng.fragment.FragmentContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
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
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(2, MockOperator.Type.STRING);
    Iterator<Object> iter = op.open(context);
    assertTrue(iter.hasNext());
    assertEquals("Mock row 0", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("Mock row 1", iter.next());
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testMockIntOperator()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(2, MockOperator.Type.INT);
    Iterator<Object> iter = op.open(context);
    assertTrue(iter.hasNext());
    assertEquals(0, iter.next());
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next());
    assertFalse(iter.hasNext());
    op.close(false);
  }

  @Test
  public void testIterator()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(2, MockOperator.Type.INT);
    int rid = 0;
    for (Object row : Operators.toIterable(op, context)) {
      assertEquals(rid++, row);
    }
    op.close(false);
  }

  // An operator is a one-pass object, don't try sequence tests that assume
  // the sequence is reentrant.
  @Test
  public void testSequenceYielder() throws IOException
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(5, MockOperator.Type.INT);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3, 4);
    Sequence<Integer> seq = Operators.toSequence(op, context);
    SequenceTestHelper.testYield("op", 5, seq, vals);
    op.close(false);
  }

  @Test
  public void testSequenceAccum() throws IOException
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(4, MockOperator.Type.INT);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
    Sequence<Integer> seq = Operators.toSequence(op, context);
    SequenceTestHelper.testAccumulation("op", seq, vals);
    op.close(false);
  }

  /**
   * Example of how the fragment context ensures dynamically-created
   * operators are closed.
   */
  @Test
  public void testContextClose()
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator op = new MockOperator(2, MockOperator.Type.INT);
    context.register(op);
    Iterator<Object> iter = op.open(context);
    assertTrue(iter.hasNext());
    assertEquals(0, iter.next());
    assertTrue(iter.hasNext());
    assertEquals(1, iter.next());
    assertFalse(iter.hasNext());
    context.close(true);
    assertEquals(Operator.State.CLOSED, op.state);
  }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   */
  @Test
  public void testSequenceOperator()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator op = new MockOperator(2, MockOperator.Type.STRING);
    Sequence<Object> seq = Operators.toSequence(op, context);
    Operator outer = Operators.toOperator(seq);
    Iterator<Object> iter = outer.open(context);
    assertTrue(iter.hasNext());
    assertEquals("Mock row 0", iter.next());
    assertTrue(iter.hasNext());
    assertEquals("Mock row 1", iter.next());
    assertFalse(iter.hasNext());
    outer.close(false);
  }
}
