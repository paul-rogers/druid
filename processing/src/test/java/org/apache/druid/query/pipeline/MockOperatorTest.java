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

package org.apache.druid.query.pipeline;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceTestHelper;
import org.apache.druid.query.pipeline.Operator.FragmentContext;
import org.junit.Test;

public class MockOperatorTest
{
  private final FragmentContext context = Operator.defaultContext();

  @Test
  public void testMockStringOperator()
  {
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
    Operator op = new MockOperator(5, MockOperator.Type.INT);
     final List<Integer> vals = Arrays.asList(0, 1, 2, 3, 4);
    Sequence<Integer> seq = Operators.toSequence(op, context);
    SequenceTestHelper.testYield("op", 5, seq, vals);
    op.close(false);
  }

  @Test
  public void testSequenceAccum() throws IOException
  {
    Operator op = new MockOperator(4, MockOperator.Type.INT);
    final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
    Sequence<Integer> seq = Operators.toSequence(op, context);
    SequenceTestHelper.testAccumulation("op", seq, vals);
    op.close(false);
  }

//   @Test
//   public void testFragmentRunner()
//   {
//     OperatorRegistry reg = new OperatorRegistry();
//     MockOperator.register(reg);
//     FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
//     MockOperator.Defn defn = new MockOperator.Defn(2, MockOperator.Defn.Type.STRING);
//     Operator op = runner.build(defn);
//     MockOperator mockOp = (MockOperator) op;
//     Iterator<Object> iter = runner.root().open();
//     assertEquals(Operator.State.RUN, mockOp.state);
//     assertTrue(iter.hasNext());
//     assertEquals("Mock row 0", iter.next());
//     assertTrue(iter.hasNext());
//     assertEquals("Mock row 1", iter.next());
//     assertFalse(iter.hasNext());
//     runner.close();
//     assertEquals(Operator.State.CLOSED, mockOp.state);
//   }

  /**
   * Example of a fragment in action, except the part of getting the
   * root operator: done here for testing, normally not needed in real
   * code.
   */
//   @Test
//   public void testFullRun()
//   {
//     OperatorRegistry reg = new OperatorRegistry();
//     MockOperator.register(reg);
//     FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
//     MockOperator.Defn defn = new MockOperator.Defn(2, MockOperator.Defn.Type.STRING);
//     Operator op = runner.build(defn);
//     MockOperator mockOp = (MockOperator) op;
//     AtomicInteger count = new AtomicInteger();
//     runner.fullRun(row -> {
//       assertEquals("Mock row " + count.getAndAdd(1), row);
//       return true;
//     });
//     assertEquals(Operator.State.CLOSED, mockOp.state);
//   }

  /**
   * Getting weird: an operator that wraps a sequence that wraps an operator.
   */
  @Test
  public void testSequenceOperator()
  {
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
