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

package org.apache.druid.exec.batch.impl;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SimpleBatchPositionerTest
{
  @Test
  public void testEmpty()
  {
    SimpleBatchPositioner positioner = new SimpleBatchPositioner();

    // Cursor has no rows.
    assertEquals(0, positioner.size());

    // Empty cursors start at EOF.
    assertEquals(0, positioner.index());
    assertTrue(positioner.isEOF());
    assertFalse(positioner.isValid());

    // Next does nothing.
    assertFalse(positioner.next());
    assertEquals(0, positioner.index());

    // Go back to the start.
    positioner.reset();
    assertEquals(0, positioner.index());

    // Seek to the end
    assertFalse(positioner.seek(0));
    assertEquals(0, positioner.index());
    assertTrue(positioner.isEOF());

    // Seek to the start.
    assertFalse(positioner.seek(-1));
    assertEquals(0, positioner.index());

    // Seek far past the end
    assertFalse(positioner.seek(10));
    assertEquals(0, positioner.index());
  }

  @Test
  public void testEmptyWithListener()
  {
    SimpleBatchPositioner positioner = new SimpleBatchPositioner();
    AtomicInteger lastPosn = new AtomicInteger(-2);
    positioner.bindListener(posn -> lastPosn.set(posn));

    // Since there is no valid position, the listener should not be called.
    assertTrue(positioner.isEOF());
    assertFalse(positioner.next());
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    positioner.reset();
    assertTrue(positioner.isEOF());
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    assertFalse(positioner.seek(1));
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    assertFalse(positioner.seek(-1));
    assertEquals(-1, lastPosn.get());
  }

  @Test
  public void testSingleValue()
  {
    SimpleBatchPositioner positioner = new SimpleBatchPositioner();
    positioner.batchBound(1);

    assertEquals(1, positioner.size());
    assertEquals(-1, positioner.index());
    assertFalse(positioner.isValid());

    assertFalse(positioner.isEOF());
    assertTrue(positioner.next());
    assertEquals(0, positioner.index());
    assertTrue(positioner.isValid());

    assertFalse(positioner.next());
    assertEquals(1, positioner.index());
    assertTrue(positioner.isEOF());
    assertFalse(positioner.isValid());

    assertFalse(positioner.next());
    assertEquals(1, positioner.index());

    positioner.reset();
    assertEquals(-1, positioner.index());

    assertFalse(positioner.seek(-1));
    assertEquals(-1, positioner.index());
    assertFalse(positioner.isEOF());

    assertFalse(positioner.seek(1));
    assertEquals(1, positioner.index());
    assertTrue(positioner.isEOF());

    assertTrue(positioner.seek(0));
    assertEquals(0, positioner.index());
    assertFalse(positioner.isEOF());
  }

  @Test
  public void testSingleValueListener()
  {
    SimpleBatchPositioner positioner = new SimpleBatchPositioner();
    positioner.batchBound(1);
    AtomicInteger lastPosn = new AtomicInteger(-2);
    positioner.bindListener(posn -> lastPosn.set(posn));

    assertEquals(1, positioner.size());
    assertEquals(-1, lastPosn.get());

    assertTrue(positioner.next());
    assertEquals(0, lastPosn.get());

    assertFalse(positioner.next());
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    assertFalse(positioner.next());
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    positioner.reset();
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    assertFalse(positioner.seek(-1));
    assertEquals(-1, lastPosn.get());

    lastPosn.set(-2);
    assertFalse(positioner.seek(1));
    assertEquals(-1, lastPosn.get());

    assertTrue(positioner.seek(0));
    assertEquals(0, lastPosn.get());
  }

  @Test
  public void testMultipleValues()
  {
    SimpleBatchPositioner positioner = new SimpleBatchPositioner();
    positioner.batchBound(10);
    AtomicInteger lastPosn = new AtomicInteger(-2);
    positioner.bindListener(posn -> lastPosn.set(posn));

    assertEquals(10, positioner.size());
    assertEquals(-1, lastPosn.get());

    assertFalse(positioner.isEOF());
    assertTrue(positioner.next());
    assertEquals(0, lastPosn.get());
    assertFalse(positioner.isEOF());

    assertTrue(positioner.next());
    assertEquals(1, lastPosn.get());
    assertFalse(positioner.isEOF());

    assertTrue(positioner.seek(9));
    assertEquals(9, lastPosn.get());
    assertFalse(positioner.isEOF());

    assertFalse(positioner.next());
    assertEquals(-1, lastPosn.get());
    assertTrue(positioner.isEOF());

    assertTrue(positioner.seek(2));
    assertEquals(2, lastPosn.get());
    assertFalse(positioner.isEOF());
  }
}
