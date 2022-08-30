package org.apache.druid.queryng.operators;

import java.util.HashMap;
import java.util.Map;

public class OperatorProfile
{
  public static final String ROW_COUNT_METRIC = "row-count";
  public static final String BATCH_COUNT_METRIC = "batch-count";
  public static final String ELAPSED_MS_METRIC = "batch-count";
  public static final String ACTIVE_MS_METRIC = "batch-count";

  private final String operatorName;
  private Map<String, Long> metrics = new HashMap<>();

  public OperatorProfile(String operatorName)
  {
    this.operatorName = operatorName;
  }

  public void add(String key, long value)
  {
    metrics.put(key, value);
  }

  public String operatorName()
  {
    return operatorName;
  }

  public Map<String, Long> metrics()
  {
    return metrics;
  }
}
