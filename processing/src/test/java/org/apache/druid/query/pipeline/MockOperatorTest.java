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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.SequenceTestHelper;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.pipeline.MockOperator.MockOperatorDef;
import org.apache.druid.query.pipeline.MockOperator.MockOperatorFactory;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class MockOperatorTest
{
   private Operator build(MockOperatorDef defn) {
     return new MockOperatorFactory().build(defn, Collections.emptyList(), FragmentRunner.defaultContext());

   }
   @Test
   public void testMockStringOperator()
   {
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.STRING);
     Operator op = build(defn);
     op.start();
     assertTrue(op.hasNext());
     assertEquals("Mock row 0", op.next());
     assertTrue(op.hasNext());
     assertEquals("Mock row 1", op.next());
     assertFalse(op.hasNext());
     op.close();
   }

   @Test
   public void testMockIntOperator()
   {
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.INT);
     Operator op = build(defn);
     op.start();
     assertTrue(op.hasNext());
     assertEquals(0, op.next());
     assertTrue(op.hasNext());
     assertEquals(1, op.next());
     assertFalse(op.hasNext());
     op.close();
   }

   @Test
   public void testIterator()
   {
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.INT);
     Operator op = build(defn);
     int rid = 0;
     for (Object row : Operators.toIterable(op)) {
       assertEquals(rid++, row);
     }
     op.close();
   }

   // An operator is a one-pass object, don't try sequence tests that assume
   // the sequence is reentrant.
   @Test
   public void testSequenceYielder() throws IOException
   {
     MockOperatorDef defn = new MockOperatorDef(5, MockOperatorDef.Type.INT);
     Operator op = build(defn);
     final List<Integer> vals = Arrays.asList(0, 1, 2, 3, 4);
     Sequence<Integer> seq = Operators.toSequence(op);
     SequenceTestHelper.testYield("op", 5, seq, vals);
     op.close();
   }

   @Test
   public void testSequenceAccum() throws IOException
   {
     MockOperatorDef defn = new MockOperatorDef(4, MockOperatorDef.Type.INT);
     Operator op = build(defn);
     final List<Integer> vals = Arrays.asList(0, 1, 2, 3);
     Sequence<Integer> seq = Operators.toSequence(op);
     SequenceTestHelper.testAccumulation("op", seq, vals);
     op.close();
   }

   @Test
   public void testFragmentRunner()
   {
     OperatorRegistry reg = new OperatorRegistry();
     reg.register(MockOperatorDef.class, new MockOperatorFactory());
     FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.STRING);
     Operator op = runner.build(defn);
     MockOperator mockOp = (MockOperator) op;
     runner.start();
     assertTrue(mockOp.started);
     assertTrue(op.hasNext());
     assertEquals("Mock row 0", op.next());
     assertTrue(op.hasNext());
     assertEquals("Mock row 1", op.next());
     assertFalse(op.hasNext());
     runner.close();
     assertTrue(mockOp.closed);
   }

   /**
    * Example of a fragment in action, except the part of getting the
    * root operator: done here for testing, normally not needed in real
    * code.
    */
   @Test
   public void testFullRun()
   {
     OperatorRegistry reg = new OperatorRegistry();
     reg.register(MockOperatorDef.class, new MockOperatorFactory());
     FragmentRunner runner = new FragmentRunner(reg, FragmentRunner.defaultContext());
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.STRING);
     Operator op = runner.build(defn);
     MockOperator mockOp = (MockOperator) op;
     AtomicInteger count = new AtomicInteger();
     runner.fullRun(row -> {
       assertEquals("Mock row " + count.getAndAdd(1), row);
       return true;
     });
     assertTrue(mockOp.started);
     assertTrue(mockOp.closed);
   }

   /**
    * Getting weird: an operator that wraps a sequence that wraps an operator.
    */
   @Test
   public void testSequenceOperator()
   {
     MockOperatorDef defn = new MockOperatorDef(2, MockOperatorDef.Type.STRING);
     Operator op = build(defn);
     Sequence<Object> seq = Operators.toSequence(op);
     Operator outer = Operators.toOperator(seq);
     outer.start();
     assertTrue(outer.hasNext());
     assertEquals("Mock row 0", outer.next());
     assertTrue(outer.hasNext());
     assertEquals("Mock row 1", outer.next());
     assertFalse(outer.hasNext());
     outer.close();
   }
}
