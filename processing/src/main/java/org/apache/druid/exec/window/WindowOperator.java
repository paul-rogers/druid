package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.OperatorProfile;
import org.apache.druid.exec.operator.Operators;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.AbstractUnaryBatchOperator;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.exec.window.old.ResultProjector;
import org.apache.druid.exec.window.old.UnpartitionedProjector;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.utils.CollectionUtils;

public class WindowOperator extends AbstractUnaryBatchOperator implements ResultIterator<Object>
{
  private final WindowSpec spec;
  private BatchBuffer batchBuffer;
  private BatchWriter<?> writer;
  private ResultProjector resultProjector;
  private int rowCount;
  private int batchCount;
  private boolean eof;

  public WindowOperator(FragmentContext context, WindowSpec spec, BatchOperator input)
  {
    super(context, buildSchema(input.batchSchema().type(), spec), input);
    this.spec = spec;
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
    writer = input.batchSchema().newWriter(spec.batchSize);
    batchBuffer = new BatchBuffer(input.batchSchema(), inputIter);
    if (CollectionUtils.isNullOrEmpty(spec.partitionKeys)) {
      resultProjector = new UnpartitionedProjector(batchBuffer, writer, spec);
    } else {
      throw new UOE("partitioning");
//      resultProjector = new PartitionedProjector(batchBuffer, writer, spec);
    }
    return this;
  }

  @Override
  public Object next() throws EofException
  {
    if (eof) {
      // EOF on previous batch
      throw Operators.eof();
    }
    writer.newBatch();
    int batchRowCount = resultProjector.writeBatch();
    eof = resultProjector.isEOF();
    if (batchRowCount == 0) {
      // EOF on this batch, with no output rows
      throw Operators.eof();
    }

    // Have rows. Save possible EOF for the next call, return results.
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
    OperatorProfile profile = new OperatorProfile("Window");
    profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
    profile.add(OperatorProfile.BATCH_COUNT_METRIC, batchCount);
    context.updateProfile(this, profile);
  }
}
