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

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.CopyProjection;
import org.apache.druid.exec.plan.WindowSpec.OffsetExpression;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.BatchVisualizer;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class BuilderTest
{
  private BatchOperator dataGen(int batchSize, int rowCount)
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rid"),
        BatchFormat.OBJECT_ARRAY,
        batchSize,
        rowCount
    );
    return new SimpleDataGenOperator(
        TestUtils.emptyFragment(),
        spec
    );
  }

  private BatchOperator groupDataGen(int batchSize, int rowCount)
  {
    SimpleDataGenSpec spec = new SimpleDataGenSpec(
        1,
        Arrays.asList("group", "rid"),
        BatchFormat.OBJECT_ARRAY,
        batchSize,
        rowCount
    );
    return new SimpleDataGenOperator(
        TestUtils.emptyFragment(),
        spec
    );
  }

  /**
   * Emulate what the Window operator does to plan the operation, build the
   * components, then write the output (which is assumed to fit in one output
   * batch.)
   */
  private Batch runPartitioner(BatchOperator inputOp, WindowSpec spec)
  {
    final ExprMacroTable macroTable = ExprMacroTable.nil();
    final BatchBuffer buffer = new BatchBuffer(inputOp.batchSchema(), inputOp.open());
    final ProjectionBuilder projBuilder = new ProjectionBuilder(buffer, macroTable, spec);
    final Builder windowBuilder = new Builder(buffer, projBuilder, spec.partitionKeys);
    final Partitioner partitioner = windowBuilder.build(spec.batchSize);
    partitioner.start();
    final BatchWriter<?> outputWriter = windowBuilder.writer;
    outputWriter.newBatch();
    partitioner.writeBatch();
    return outputWriter.harvestAsBatch();
  }

  private WindowSpec unpartitionedSpec(List<OutputColumn> cols)
  {
    return new WindowSpec(
        1,     // ID - not used
        2,     // Child - not used
        1000,  // Batch Size
        cols,  // Output columns
        null   // Partition keys
    );
  }

  private WindowSpec partitionedSpec(List<OutputColumn> cols)
  {
    return new WindowSpec(
        1,     // ID - not used
        2,     // Child - not used
        1000,  // Batch Size
        cols,  // Output columns
        Collections.singletonList("group")   // Partition keys
    );
  }

  @Test
  public void testPrimaryOnlyEmptyInput()
  {
    List<OutputColumn> cols = Collections.singletonList(
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    final int rowCount = 0;
    Batch output = runPartitioner(dataGen(5, rowCount), unpartitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testPrimaryOnly()
  {
    List<OutputColumn> cols = Collections.singletonList(
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    final int rowCount = 4;
    Batch output = runPartitioner(dataGen(5, rowCount), unpartitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1)
        .row(2)
        .row(3)
        .row(4)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testLead()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    final int rowCount = 4;
    Batch output = runPartitioner(dataGen(5, rowCount), unpartitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 3)
        .row(2, 4)
        .row(3, null)
        .row(4, null)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testLagOnly()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2)
    );
    final int rowCount = 4;
    Batch output = runPartitioner(dataGen(5, rowCount), unpartitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, null)
        .row(2, null)
        .row(3, 1)
        .row(4, 2)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testLeadAndLag()
  {
    List<OutputColumn> cols = Arrays.<OutputColumn>asList(
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    final int rowCount = 4;
    Batch output = runPartitioner(dataGen(5, rowCount), unpartitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, null, 3)
        .row(2, null, 4)
        .row(3, 1, null)
        .row(4, 2, null)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testPartitionedPrimaryOnlyEmptyInput()
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    final int rowCount = 0;
    Batch output = runPartitioner(groupDataGen(5, rowCount), partitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("group", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  /**
   * Simplest partitioned test: only the primary reader. THis is not actually useful
   * as the output is no different than the input because partitioning does not
   * affect anything.
   */
  @Test
  public void testPartitionedPrimaryOnly()
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    final int rowCount = 9;
    Batch output = runPartitioner(groupDataGen(5, rowCount), partitionedSpec(cols));
    BatchVisualizer.print(output);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("group", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .build();
    BatchBuilder batchBuilder = BatchBuilder.arrayList(expectedSchema);
    for (int i = 0; i < rowCount; i++) {
      batchBuilder.row("Group " + (i / 5 + 1), i + 1);
    }
    Batch expected = batchBuilder.build();
    BatchValidator.assertEquals(expected, output);
  }

  /**
   * Partitioning with a LEAD expression. The lead restarts at each partition boundary.
   * Here, the groups are aligned with batch size: each is 5 rows long.
   */
  @Test
  public void testPartitionedLeadBatchAligned()
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    final int rowCount = 9;
    Batch output = runPartitioner(groupDataGen(5, rowCount), partitionedSpec(cols));

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("group", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row("Group 1", 1, 3)
        .row("Group 1", 2, 4)
        .row("Group 1", 3, 5)
        .row("Group 1", 4, null)
        .row("Group 1", 5, null)
        .row("Group 2", 6, 8)
        .row("Group 2", 7, 9)
        .row("Group 2", 8, null)
        .row("Group 2", 9, null)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  @Test
  public void testPartitionedLagBatchAligned()
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2)
    );
    Batch output = runPartitioner(groupDataGen(5, 9), partitionedSpec(cols));
    validateLeadLag(output);
  }

  private void validateLeadLag(Batch output)
  {
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("group", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row("Group 1", 1, null)
        .row("Group 1", 2, null)
        .row("Group 1", 3, 1)
        .row("Group 1", 4, 2)
        .row("Group 1", 5, 3)
        .row("Group 2", 6, null)
        .row("Group 2", 7, null)
        .row("Group 2", 8, 6)
        .row("Group 2", 9, 7)
        .build();
    BatchValidator.assertEquals(expected, output);
  }

  public void testPartitionedLeadAndLag(int batchSize)
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid"),
        new OffsetExpression("rid-2", ColumnType.LONG, "rid", -2),
        new OffsetExpression("rid+2", ColumnType.LONG, "rid", 2)
    );
    Batch output = runPartitioner(groupDataGen(batchSize, 9), partitionedSpec(cols));
    BatchVisualizer.print(output);

    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("group", ColumnType.STRING)
        .scalar("rid", ColumnType.LONG)
        .scalar("rid-2", ColumnType.LONG)
        .scalar("rid+2", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row("Group 1", 1, null, 3)
        .row("Group 1", 2, null, 4)
        .row("Group 1", 3, 1, 5)
        .row("Group 1", 4, 2, null)
        .row("Group 1", 5, 3, null)
        .row("Group 2", 6, null, 8)
        .row("Group 2", 7, null, 9)
        .row("Group 2", 8, 6, null)
        .row("Group 2", 9, 7, null)
        .build();
    BatchValidator.assertEquals(expected, output);
    BatchVisualizer.print(output);
  }

  @Test
  public void testPartitionedLeadAndLagBatchAligned()
  {
    testPartitionedLeadAndLag(5);
  }

  /**
   * As above, but with a batch size smaller than the partition size. Tests detecting a
   * partition in one batch, and carrying the partition into the next batch.
   */
  @Test
  public void testPartitionedLeadAndLagNonalignedBatch1()
  {
    testPartitionedLeadAndLag(4);
  }

  /**
   * As above, but with a batch size greater than the partition size. Tests carrying
   * a partition forward across batches, then detecting a new partition in the next
   * batch.
   */
  @Test
  public void testPartitionedLeadAndLagNonalignedBatch2()
  {
    testPartitionedLeadAndLag(6);
  }

  @Test
  public void testPartitionedLeadAndLagTinyBatch()
  {
    testPartitionedLeadAndLag(1);
  }

  @Test
  public void testPartitionedLeadAndLagSingeBatch()
  {
    testPartitionedLeadAndLag(10);
  }
}
