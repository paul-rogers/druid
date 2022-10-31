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

public class SeekableCursorTest
{
  @Test
  public void testEmpty()
  {
    SeekableCursor cursor = new SeekableCursor();

    // Cursor has no rows.
    assertEquals(0, cursor.size());

    // Empty cursors start at EOF.
    assertEquals(0, cursor.index());
    assertTrue(cursor.isEOF());
    assertFalse(cursor.isValid());

    // Next does nothing.
    assertFalse(cursor.next());
    assertEquals(0, cursor.index());

    // Go back to the start.
    cursor.reset();
    assertEquals(0, cursor.index());

    // Seek to the end
    assertFalse(cursor.seek(0));
    assertEquals(0, cursor.index());
    assertTrue(cursor.isEOF());

    // Seek to the start.
    assertFalse(cursor.seek(-1));
    assertEquals(0, cursor.index());

    // Seek far past the end
    assertFalse(cursor.seek(10));
    assertEquals(0, cursor.index());
  }

  @Test
  public void testEmptyListener()
  {
    SeekableCursor cursor = new SeekableCursor();
    AtomicInteger lastPosn = new AtomicInteger(-2);
    cursor.bindListener(posn -> lastPosn.set(posn));

    // Since there is no valid position, the listener should not be called.
    assertTrue(cursor.isEOF());
    assertFalse(cursor.next());
    assertEquals(-2, lastPosn.get());

    cursor.reset();
    assertTrue(cursor.isEOF());
    assertEquals(-2, lastPosn.get());

    assertFalse(cursor.seek(1));
    assertEquals(-2, lastPosn.get());

    assertFalse(cursor.seek(-1));
    assertEquals(-2, lastPosn.get());
  }

  @Test
  public void testSingleValue()
  {
    SeekableCursor cursor = new SeekableCursor();
    cursor.bind(1);

    assertEquals(1, cursor.size());
    assertEquals(-1, cursor.index());
    assertFalse(cursor.isValid());

    assertFalse(cursor.isEOF());
    assertTrue(cursor.next());
    assertEquals(0, cursor.index());
    assertTrue(cursor.isValid());

    assertFalse(cursor.next());
    assertEquals(1, cursor.index());
    assertTrue(cursor.isEOF());
    assertFalse(cursor.isValid());

    assertFalse(cursor.next());
    assertEquals(1, cursor.index());

    cursor.reset();
    assertEquals(-1, cursor.index());

    assertFalse(cursor.seek(-1));
    assertEquals(-1, cursor.index());
    assertFalse(cursor.isEOF());

    assertFalse(cursor.seek(1));
    assertEquals(1, cursor.index());
    assertTrue(cursor.isEOF());

    assertTrue(cursor.seek(0));
    assertEquals(0, cursor.index());
    assertFalse(cursor.isEOF());
  }

  @Test
  public void testSingleValueListener()
  {
    SeekableCursor cursor = new SeekableCursor();
    cursor.bind(1);
    AtomicInteger lastPosn = new AtomicInteger(-2);
    cursor.bindListener(posn -> lastPosn.set(posn));

    assertEquals(1, cursor.size());
    assertEquals(-2, lastPosn.get());

    assertTrue(cursor.next());
    assertEquals(0, lastPosn.get());

    assertFalse(cursor.next());
    assertEquals(0, lastPosn.get());

    assertFalse(cursor.next());
    assertEquals(0, lastPosn.get());

    cursor.reset();
    assertEquals(0, lastPosn.get());

    assertFalse(cursor.seek(-1));
    assertEquals(0, lastPosn.get());

    assertFalse(cursor.seek(1));
    assertEquals(0, lastPosn.get());

    assertTrue(cursor.seek(0));
    assertEquals(0, lastPosn.get());
  }

  @Test
  public void testMultipleValues()
  {
    SeekableCursor cursor = new SeekableCursor();
    cursor.bind(10);
    AtomicInteger lastPosn = new AtomicInteger(-2);
    cursor.bindListener(posn -> lastPosn.set(posn));

    assertEquals(10, cursor.size());
    assertEquals(-2, lastPosn.get());

    assertFalse(cursor.isEOF());
    assertTrue(cursor.next());
    assertEquals(0, lastPosn.get());
    assertFalse(cursor.isEOF());

    assertTrue(cursor.next());
    assertEquals(1, lastPosn.get());
    assertFalse(cursor.isEOF());

    assertTrue(cursor.seek(9));
    assertEquals(9, lastPosn.get());
    assertFalse(cursor.isEOF());

    assertFalse(cursor.next());
    assertEquals(9, lastPosn.get());
    assertTrue(cursor.isEOF());

    assertTrue(cursor.seek(2));
    assertEquals(2, lastPosn.get());
    assertFalse(cursor.isEOF());
  }
}
