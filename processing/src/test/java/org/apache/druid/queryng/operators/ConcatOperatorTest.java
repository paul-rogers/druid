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

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.Operator.EofException;
import org.apache.druid.queryng.operators.Operator.ResultIterator;
import org.apache.druid.queryng.operators.Operator.State;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

@Category(OperatorTest.class)
public class ConcatOperatorTest
{
  @Test
  public void testEmpty()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<String> op = new ConcatOperator<String>(context, Collections.emptyList());
    List<String> results = Operators.toList(op);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testOneEmptyInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<String> input = new NullOperator<String>(context);
    Operator<String> op = new ConcatOperator<String>(
        context,
        Collections.singletonList(input));
    List<String> results = Operators.toList(op);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testHelperNoConcat()
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator<Integer> input1 = MockOperator.ints(context, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(context, Collections.singletonList(input1));
    assertSame(input1, op);
  }

  @Test
  public void testTwoEmptyInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<String> input1 = new NullOperator<String>(context);
    Operator<String> input2 = new NullOperator<String>(context);
    Operator<String> op = new ConcatOperator<String>(
        context,
        Arrays.asList(input1, input2));
    List<String> results = Operators.toList(op);
    assertTrue(results.isEmpty());
  }

  @Test
  public void testOneInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    Operator<Integer> op = new ConcatOperator<Integer>(
        context,
        Collections.singletonList(input));
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testEmptyThenNonEmptyInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input1 = new NullOperator<Integer>(context);
    Operator<Integer> input2 = MockOperator.ints(context, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        context,
        Arrays.asList(input1, input2));
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testNonEmptyThenEmptyInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input1 = MockOperator.ints(context, 2);
    Operator<Integer> input2 = new NullOperator<Integer>(context);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        context,
        Arrays.asList(input1, input2));
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testTwoInputs()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input1 = MockOperator.ints(context, 2);
    Operator<Integer> input2 = MockOperator.ints(context, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        context,
        Arrays.asList(input1, input2));
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1, 0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testClose() throws EofException
  {
    FragmentContext context = FragmentContext.defaultContext();
    MockOperator<Integer> input1 = MockOperator.ints(context, 2);
    MockOperator<Integer> input2 = MockOperator.ints(context, 2);
    Operator<Integer> op = ConcatOperator.concatOrNot(
        context,
        Arrays.asList(input1, input2));
    ResultIterator<Integer> iter = op.open();
    assertEquals(0, (int) iter.next());

    // Only first input has been opened.
    assertEquals(State.RUN, input1.state);
    assertEquals(State.START, input2.state);

    // Cascade closes inputs
    op.close(true);
    assertEquals(State.CLOSED, input1.state);
    assertEquals(State.START, input2.state);

    // Close again does nothing.
    op.close(false);
  }
}
