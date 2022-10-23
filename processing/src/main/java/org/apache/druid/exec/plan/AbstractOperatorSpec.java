package org.apache.druid.exec.plan;

import org.apache.druid.exec.operator.OperatorSpec;

import java.util.Collections;
import java.util.List;

public abstract class AbstractOperatorSpec implements OperatorSpec
{
  private final int id;

  public AbstractOperatorSpec(int id)
  {
    this.id = id;
  }

  @Override
  public int id()
  {
    return id;
  }

  @Override
  public List<OperatorSpec> children()
  {
    return Collections.emptyList();
  }
}
