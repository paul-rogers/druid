package org.apache.druid.exec.plan;

import org.apache.druid.exec.operator.OperatorSpec;
import org.apache.druid.frame.key.SortColumn;

import java.util.List;

public class InternalSortOp extends AbstractUnarySpec
{
  public enum SortType
  {
    ROW
  }

  private final SortType sortType;
  private final List<SortColumn> keys;

  public InternalSortOp(int id, OperatorSpec child, SortType sortType, List<SortColumn> keys)
  {
    super(id, child);
    this.sortType = sortType;
    this.keys = keys;
  }

  public SortType sortType()
  {
    return sortType;
  }

  public List<SortColumn> keys()
  {
    return keys;
  }
}
