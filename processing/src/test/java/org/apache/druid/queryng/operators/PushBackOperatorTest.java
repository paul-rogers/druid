package org.apache.druid.queryng.operators;

import org.apache.druid.queryng.fragment.FragmentContext;
import org.junit.Test;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PushBackOperatorTest
{
  @Test
  public void testEmptyInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<String> input = new NullOperator<String>(context);
    Operator<String> op = new PushBackOperator<String>(context, input);
    assertTrue(Operators.toList(op).isEmpty());
  }

  @Test
  public void testSimpleInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    Operator<Integer> op = new PushBackOperator<Integer>(context, input);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testPush()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(context, input);
    Iterator<Integer> iter = op.open();
    assertTrue(iter.hasNext());
    Integer item = iter.next();
    op.push(item);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }

  @Test
  public void testInitialPush()
  {
    FragmentContext context = FragmentContext.defaultContext();
    Operator<Integer> input = MockOperator.ints(context, 2);
    Iterator<Integer> iter = input.open();
    assertTrue(iter.hasNext());
    Integer item = iter.next();
    PushBackOperator<Integer> op = new PushBackOperator<Integer>(context, input, iter, item);
    List<Integer> results = Operators.toList(op);
    List<Integer> expected = Arrays.asList(0, 1);
    assertEquals(expected, results);
  }
}
