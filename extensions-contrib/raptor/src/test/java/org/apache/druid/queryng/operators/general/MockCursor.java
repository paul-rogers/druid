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

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.LongColumnSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.DateTime;
import org.joda.time.Interval;

public class MockCursor implements Cursor, ColumnSelectorFactory
{
  private class MockTimeColumn implements LongColumnSelector
  {
    @Override
    public long getLong()
    {
      return startTime + posn * msPerRow;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public boolean isNull()
    {
      return false;
    }
  }

  private class MockLongColumn implements LongColumnSelector
  {
    @Override
    public long getLong()
    {
      return posn % 100;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public boolean isNull()
    {
      return false;
    }
  }

  private abstract class MockStringColumn implements ColumnValueSelector<String>
  {
    @Override
    public long getLong()
    {
      return 0;
    }

    @Override
    public void inspectRuntimeShape(RuntimeShapeInspector inspector)
    {
    }

    @Override
    public boolean isNull()
    {
      return false;
    }

    @Override
    public double getDouble()
    {
      return 0;
    }

    @Override
    public float getFloat()
    {
      return 0;
    }

    @Override
    public Class<String> classOfObject()
    {
      return String.class;
    }
  }

  private class MockRowLabelColumn extends MockStringColumn
  {
    @Override
    public String getObject()
    {
      return "row " + (posn + 1);
    }
  }

  private class MockDimColumn extends MockStringColumn
  {
    @Override
    public String getObject()
    {
      return "dim " + (posn % 3 + 1);
    }
  }

  private final MockStorageAdapter adapter;
  private final int targetRowCount;
  private final long startTime;
  private final int msPerRow;
  private int posn;

  public MockCursor(
      final MockStorageAdapter adapter,
      final Interval interval,
      final int segmentSize
  )
  {
    this.adapter = adapter;
    this.startTime = interval.getStartMillis();
    this.targetRowCount = segmentSize;
    long span = interval.getEndMillis() - startTime;

    // A zero-row cursor is a bit silly, but is needed to test the
    // zero-row path.
    if (targetRowCount == 0) {
      this.msPerRow = 1;
    } else {
      this.msPerRow = (int) Math.max(1, span / targetRowCount);
    }
  }

  @Override
  public ColumnSelectorFactory getColumnSelectorFactory()
  {
    return this;
  }

  @Override
  public DimensionSelector makeDimensionSelector(
      DimensionSpec dimensionSpec)
  {
    throw new ISE("Not supported");
  }

  @Override
  public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
  {
    switch (columnName) {
      case ColumnHolder.TIME_COLUMN_NAME:
        return new MockTimeColumn();
      case "delta":
        return new MockLongColumn();

      // Unique column for each row
      case "page":
        return new MockRowLabelColumn();

      // Repeats every three rows
      case "dim":
        return new MockDimColumn();
      default:
        return null;
    }
  }

  @Override
  public ColumnCapabilities getColumnCapabilities(String column)
  {
    return adapter.getColumnCapabilities(column);
  }

  @Override
  public DateTime getTime()
  {
    return DateTimes.utc(startTime + posn * msPerRow);
  }

  @Override
  public void advance()
  {
    posn++;
  }

  @Override
  public void advanceUninterruptibly()
  {
    advance();
  }

  @Override
  public boolean isDone()
  {
    return posn >= targetRowCount;
  }

  @Override
  public boolean isDoneOrInterrupted()
  {
    return isDone();
  }

  @Override
  public void reset()
  {
    posn = 0;
  }
}
