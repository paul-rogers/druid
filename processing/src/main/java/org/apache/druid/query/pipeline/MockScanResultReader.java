package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.column.ColumnHolder;
import org.joda.time.Interval;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;

public class MockScanResultReader implements Operator {
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      return new MockScanResultReader((Defn) defn);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends LeafDefn
  {
    public String segmentId = "mock-segment";
    public List<String> columns;
    public ResultFormat resultFormat;
    public int rowCount;
    public int batchSize = 3;
    public Interval interval;

    public Defn(int columnCount, int rowCount, Interval interval) {
      this.interval = interval;
      this.columns = new ArrayList<>(columnCount);
      if (columnCount > 0) {
        columns.add(ColumnHolder.TIME_COLUMN_NAME);
      }
      for (int i = 1; i < columnCount;  i++) {
        columns.add("Column" + Integer.toString(i));
      }
      this.rowCount = rowCount;
      this.resultFormat = ResultFormat.RESULT_FORMAT_COMPACTED_LIST;
    }
  }

  private final Defn defn;
  private final long msPerRow;
  private long nextTs;
  private int rowCount;
  @SuppressWarnings("unused")
  private int batchCount;
  /**
   * State allows tests to verify that the operator protocol
   * was followed. Not really necessary here functionally, so this
   * is just a test tool.
   */
  public State state = State.START;

  public MockScanResultReader(Defn defn)
  {
    this.defn = defn;
    if (defn.rowCount == 0) {
      this.msPerRow = 0;
    } else {
      this.msPerRow = Math.toIntExact(defn.interval.toDurationMillis() / defn.rowCount);
    }
    this.nextTs = defn.interval.getStartMillis();
  }

  @Override
  public void start()
  {
    state = State.RUN;
  }

  @Override
  public boolean hasNext() {
    Preconditions.checkState(state == State.RUN);
    return rowCount < defn.rowCount;
  }

  @Override
  public Object next() {
    int n = Math.min(defn.rowCount - rowCount, defn.batchSize);
    List<List<Object>> batch = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      List<Object> values = new ArrayList<>(defn.columns.size());
      if (!defn.columns.isEmpty()) {
        values.add(nextTs);
      }
      for (int j = 1; j < defn.columns.size(); j++) {
        values.add(StringUtils.format("Value %d.%d", rowCount, j));
      }
      batch.add(values);
      rowCount++;
      nextTs += msPerRow;
    }
    batchCount++;
    return new ScanResultValue(defn.segmentId, defn.columns, batch);
  }

  @Override
  public void close(boolean cascade)
  {
    state = State.CLOSED;
  }

  @VisibleForTesting
  public static long getTime(List<Object> row) {
    return (Long) row.get(0);
  }

  @VisibleForTesting
  public static long getFirstTime(Object row) {
    ScanResultValue value = (ScanResultValue) row;
    List<List<Object>> events = value.getRows();
    if (events.isEmpty()) {
      return 0;
    }
    return getTime(events.get(0));
  }

  @VisibleForTesting
  public static long getLastTime(Object row) {
    ScanResultValue value = (ScanResultValue) row;
    List<List<Object>> events = value.getRows();
    if (events.isEmpty()) {
      return 0;
    }
    return getTime(events.get(events.size() - 1));
  }
}
