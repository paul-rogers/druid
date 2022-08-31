package org.apache.druid.queryng.operators;

import java.util.HashMap;
import java.util.Map;

public class OperatorProfile
{
  public static final String ROW_COUNT_METRIC = "row-count";
  public static final String BATCH_COUNT_METRIC = "batch-count";
  public static final String ELAPSED_MS_METRIC = "batch-count";
  public static final String ACTIVE_MS_METRIC = "batch-count";
  public static final String CPU_TIME_NS = "cpu-time-ns";

  public final String operatorName;
  public boolean omitFromProfile;
  private Map<String, Long> metrics = new HashMap<>();

  public OperatorProfile(String operatorName)
  {
    this.operatorName = operatorName;
    this.omitFromProfile = false;
  }

  public OperatorProfile(String operatorName, boolean omit)
  {
    this.operatorName = operatorName;
    this.omitFromProfile = omit;
  }

  public static OperatorProfile silentOperator(String name)
  {
    return new OperatorProfile(name, true);
  }

  public static OperatorProfile silentOperator(Operator<?> op)
  {
    return silentOperator(op.getClass().getSimpleName());
  }

  public void add(String key, long value)
  {
    metrics.put(key, value);
  }

  public Map<String, Long> metrics()
  {
    return metrics;
  }
}
