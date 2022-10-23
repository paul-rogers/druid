package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.BatchReader.BatchCursor;
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
    BatchCursor cursor = new SeekableCursor();

    // Cursor has no rows.
    assertEquals(0, cursor.size());

    // Empty cursors start at EOF.
    assertEquals(0, cursor.index());
    assertTrue(cursor.isEOF());

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

    assertFalse(cursor.isEOF());
    assertTrue(cursor.next());
    assertEquals(0, cursor.index());

    assertFalse(cursor.next());
    assertEquals(1, cursor.index());
    assertTrue(cursor.isEOF());

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
