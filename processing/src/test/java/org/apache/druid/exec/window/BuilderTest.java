package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.plan.WindowSpec;
import org.apache.druid.exec.plan.WindowSpec.OutputColumn;
import org.apache.druid.exec.plan.WindowSpec.CopyProjection;
import org.apache.druid.exec.plan.WindowSpec.OffsetExpression;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.BatchVisualizer;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.exec.window.old.WindowFrameSequencer;
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

  @Test
  public void testPartitionedPrimaryOnly()
  {
    List<OutputColumn> cols = Arrays.asList(
        new CopyProjection("group", ColumnType.STRING, "group"),
        new CopyProjection("rid", ColumnType.LONG, "rid")
    );
    final int rowCount = 9;
    Batch output = runPartitioner(groupDataGen(5, rowCount), partitionedSpec(cols));

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

}
