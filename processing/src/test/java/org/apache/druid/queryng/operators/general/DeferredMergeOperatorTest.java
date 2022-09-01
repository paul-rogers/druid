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

import com.google.common.collect.Ordering;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.AbstractMergeOperator.Input;
import org.apache.druid.queryng.operators.DeferredMergeOperator;
import org.apache.druid.queryng.operators.Iterators;
import org.apache.druid.queryng.operators.MockOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.Operator.EofException;
import org.apache.druid.queryng.operators.Operator.ResultIterator;
import org.apache.druid.queryng.operators.Operator.State;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.OperatorTests;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(OperatorTest.class)
public class DeferredMergeOperatorTest
{
  @Test
  public void testNoInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> op = new DeferredMergeOperator<>(
        context,
        Ordering.natural(),
        0,
        Collections.emptyList()
    );
    ResultIterator<Integer> iter = op.open();
    OperatorTests.assertEof(iter);
    op.close(true);
  }

  private static <T> Input<T> makeInput(Operator<T> op)
  {
    try {
      ResultIterator<T> iter = op.open();
      return new Input<T>(op, iter, iter.next());
    }
    catch (EofException e) {
      fail();
      return null;
    }
  }

  @Test
  public void testOneInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> op = new DeferredMergeOperator<>(
        context,
        Ordering.natural(),
        1,
        Collections.singletonList(
            makeInput(MockOperator.ints(context, 3))
        )
    );
    ResultIterator<Integer> iter = op.open();
    List<Integer> results = Iterators.toList(iter);
    op.close(true);
    assertEquals(Arrays.asList(0, 1, 2), results);
  }

  @Test
  public void testTwoInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> op = new DeferredMergeOperator<>(
        context,
        Ordering.natural(),
        2,
        Arrays.asList(
            makeInput(MockOperator.ints(context, 3)),
            makeInput(MockOperator.ints(context, 5))
        )
    );
    ResultIterator<Integer> iter = op.open();
    List<Integer> results = Iterators.toList(iter);
    op.close(true);
    assertEquals(Arrays.asList(0, 0, 1, 1, 2, 2, 3, 4), results);
  }

  @Test
  public void testClose()
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator<Integer> input1 = MockOperator.ints(context, 2);
    MockOperator<Integer> input2 = MockOperator.ints(context, 2);
    Operator<Integer> op = new DeferredMergeOperator<>(
        context,
        Ordering.natural(),
        2,
        Arrays.asList(
            makeInput(input1),
            makeInput(input2)
        )
    );
    ResultIterator<Integer> iter = op.open();
    List<Integer> results = Iterators.toList(iter);
    assertEquals(Arrays.asList(0, 0, 1, 1), results);

    // Inputs are closed as exhausted.
    assertTrue(input1.state == State.CLOSED);
    assertTrue(input2.state == State.CLOSED);
    op.close(true);
  }
}
