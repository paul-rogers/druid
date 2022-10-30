package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;

public abstract class AbstractBatchOperator implements BatchOperator
{
  protected final FragmentContext context;
  protected final BatchSchema schema;

  public AbstractBatchOperator(final FragmentContext context, final BatchSchema schema)
  {
    this.context = context;
    this.schema = schema;
  }

  @Override
  public BatchSchema batchSchema()
  {
    return schema;
  }
}
