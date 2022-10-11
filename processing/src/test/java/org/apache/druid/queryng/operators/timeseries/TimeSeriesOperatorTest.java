package org.apache.druid.queryng.operators.timeseries;

import org.apache.druid.common.config.NullHandling;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.Result;
import org.apache.druid.query.aggregation.CountAggregatorFactory;
import org.apache.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.query.timeseries.TimeseriesResultValue;
import org.apache.druid.queryng.fragment.FragmentManager;
import org.apache.druid.queryng.fragment.Fragments;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.general.CursorDefinition;
import org.apache.druid.queryng.operators.general.MockStorageAdapter;
import org.apache.druid.queryng.operators.general.MockStorageAdapter.MockSegment;
import org.apache.druid.segment.VirtualColumns;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TimeSeriesOperatorTest
{
  static {
    NullHandling.initializeForTests();
  }

  private CursorDefinition mockCursor(int rowCount, int cursorCount, boolean sparse)
  {
    return new CursorDefinition(
        new MockSegment(rowCount, cursorCount, sparse),
        MockStorageAdapter.MOCK_INTERVAL, // Whole segment
        null, // No filter
        VirtualColumns.EMPTY,
        false,
        Granularities.HOUR,
        null, // No query metrics
        0 // No vector
    );
  }

  @Test
  public void testBasics()
  {
    FragmentManager fragment = Fragments.defaultFragment();
    Operator<Result<TimeseriesResultValue>> op = new TimeseriesEngineOperator(
        fragment,
        mockCursor(4, 1, false),
        Arrays.asList(
            new CountAggregatorFactory("cnt"),
            new LongSumAggregatorFactory("sum", "delta")
        ),
        null, // No buffer pool: not vectorized
        false // don't skip empty buckets
    );
    fragment.registerRoot(op);
    List<Result<TimeseriesResultValue>> results = fragment.toList();
    System.out.println(results);
    assertEquals(1, results.size());
    Result<TimeseriesResultValue> value = results.get(0);
    assertEquals(MockStorageAdapter.MOCK_INTERVAL.getStart(), value.getTimestamp());
  }

  @Test
  public void testEmptyResults()
  {

  }

  @Test
  public void testSingleRowPerGrainResults()
  {

  }

  @Test
  public void testRowAggWithinGrain()
  {

  }

  @Test
  public void testSparseRowsWithZeroFill()
  {

  }


  @Test
  public void testSparseRowsWithoutZeroFill()
  {

  }
}
