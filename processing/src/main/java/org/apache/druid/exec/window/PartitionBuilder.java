package org.apache.druid.exec.window;

import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.utils.CollectionUtils;

import java.util.List;

public class PartitionBuilder
{
  private final WindowSpec spec;

  public void build(List<WindowFrameCursor> cursors)
  {
    if (CollectionUtils.isNullOrEmpty(spec.partitionKeys)) {
      buildUnpartitioned(cursors);
    } else {
      buildPartitioned(cursors);
    }
  }

  private void buildUnpartitioned(List<WindowFrameCursor> cursors)
  {
    // TODO Auto-generated method stub

  }

  private void buildPartitioned(List<WindowFrameCursor> cursors)
  {
    // TODO Auto-generated method stub

  }
}
