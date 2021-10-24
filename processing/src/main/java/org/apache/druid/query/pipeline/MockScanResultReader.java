package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;

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

    public Defn(int columnCount, int rowCount) {
      this.columns = new ArrayList<>(columnCount);
      for (int i = 0; i < columnCount;  i++) {
        columns.add("Column" + Integer.toString(i + 1));
      }
      this.rowCount = rowCount;
      this.resultFormat = ResultFormat.RESULT_FORMAT_COMPACTED_LIST;
    }
  }

  private final Defn defn;
  private int rowCount;
  @SuppressWarnings("unused")
  private int batchCount;

  public MockScanResultReader(Defn defn)
  {
    this.defn = defn;
  }

  @Override
  public void start()
  {
  }

  @Override
  public boolean hasNext() {
    return rowCount < defn.rowCount;
  }

  @Override
  public Object next() {
    int n = Math.min(defn.rowCount - rowCount, defn.batchSize);
    List<List<String>> batch = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      List<String> values = new ArrayList<>(defn.columns.size());
      for (int j = 0; j < defn.columns.size(); j++) {
        values.add(StringUtils.format("Value %d.%d", rowCount, j));
      }
      batch.add(values);
      rowCount++;
    }
    batchCount++;
    return new ScanResultValue(defn.segmentId, defn.columns, batch);
  }

  @Override
  public void close(boolean cascade)
  {
  }
}
