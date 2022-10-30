package org.apache.druid.exec.operator;

import org.apache.druid.exec.batch.BatchSchema;

public interface BatchOperator extends Operator<Object>
{
  BatchSchema batchSchema();
}
