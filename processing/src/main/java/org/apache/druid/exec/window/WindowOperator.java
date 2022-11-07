/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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

public class WindowOperator extends AbstractUnaryBatchOperator implements ResultIterator<Object>
{
  private final WindowSpec spec;
  private BatchBuffer batchBuffer;
  private BatchWriter<?> writer;
  private Partitioner partitioner;
  private int rowCount;
  private int batchCount;

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
    final ProjectionBuilder projBuilder = new ProjectionBuilder(
        batchBuffer,
        context.macroTable(),
        spec
    );
    final Builder windowBuilder = new Builder(batchBuffer, projBuilder, spec.partitionKeys);
    partitioner = windowBuilder.build(spec.batchSize);
    partitioner.start();
    writer = windowBuilder.writer;
    return this;
  }

  @Override
  public Object next() throws EofException
  {
    if (partitioner.isEOF()) {
      // EOF on previous batch
      throw Operators.eof();
    }
    writer.newBatch();
    int batchRowCount = partitioner.writeBatch();
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
