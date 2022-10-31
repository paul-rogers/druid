package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.Operators;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractUnaryBatchOperator;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.util.SchemaBuilder;

public class WindowOperator extends AbstractUnaryBatchOperator implements ResultIterator<Object>
{
  private BatchBuffer batchBuffer;
  private BatchWriter<?> writer;
  private Partitioner partitioner;
  private int rowCount;
  private int batchCount;

  public WindowOperator(FragmentContext context, WindowSpec spec, BatchOperator input)
  {
    super(context, buildSchema(input.batchSchema().type(), spec), input);
  }

  private static BatchSchema buildSchema(BatchType inputType, WindowSpec spec)
  {
    SchemaBuilder schemaBuilder = new SchemaBuilder();
    for (OutputColumn col : spec.columns) {
      schemaBuilder.addScalar(col.name, col.dataType);
    }
    return schemaBuilder.buildBatchSchema(inputType);
  }

  @Override
  public ResultIterator<Object> open()
  {
    openInput();
    batchBuffer = new BatchBuffer(input.batchSchema(), inputIter);
    partitioner = new Partitioner(batchBuffer);
    return this;
  }

  @Override
  public Object next() throws EofException
  {
    int batchRowCount = 0;
    while (!writer.isFull()) {
      int writeCount = partitioner.writeBatch();
      batchRowCount += writeCount;
      if (writeCount == 0) {
        break;
      }
    }
    if (batchRowCount == 0) {
      throw Operators.eof();
    }
    rowCount += batchRowCount;
    batchCount++;
    return writer.harvest();
  }

  @Override
  public void close(boolean cascade)
  {
    if (cascade) {
      closeInput();
    }

  }
}
