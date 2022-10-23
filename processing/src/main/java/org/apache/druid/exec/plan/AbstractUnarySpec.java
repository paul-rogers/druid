package org.apache.druid.exec.plan;

import org.apache.druid.exec.operator.OperatorSpec;

import java.util.Collections;
import java.util.List;

public class AbstractUnarySpec extends AbstractOperatorSpec
{
  private final OperatorSpec child;

  public AbstractUnarySpec(int id, OperatorSpec child)
  {
    super(id);
    this.child = child;
  }

  @Override
  public List<OperatorSpec> children()
  {
    return Collections.singletonList(child);
  }
}
