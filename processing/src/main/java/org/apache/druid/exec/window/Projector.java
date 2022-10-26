package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.batch.ColumnWriterFactory.ScalarColumnWriter;
import org.apache.druid.exec.batch.RowReader.RowCursor;
import org.apache.druid.exec.window.BatchBuffer.PartitionRange;
import org.apache.druid.exec.window.BatchBuffer.PartitionReader;

import java.util.List;

public class Projector
{
  public interface ColumnProjector
  {
    void project();
  }

  protected static abstract class AbstractColumnProjector implements ColumnProjector
  {
    protected final ScalarColumnWriter destColumn;

    protected AbstractColumnProjector(final ScalarColumnWriter destColumn)
    {
      this.destColumn = destColumn;
    }
  }

  protected static class NullProjector extends AbstractColumnProjector
  {
    protected NullProjector(final ScalarColumnWriter destColumn)
    {
      super(destColumn);
    }

    @Override
    public void project()
    {
      destColumn.setNull();
    }
  }

  // TODO: Change to be type-specific to be more efficient for frames.
  protected static class LiteralProjector extends AbstractColumnProjector
  {
    private final Object literal;

    protected LiteralProjector(final ScalarColumnWriter destColumn, final Object literal)
    {
      super(destColumn);
      this.literal = literal;
    }

    @Override
    public void project()
    {
      destColumn.setValue(literal);
    }
  }

//  public static class AggregateProjector implements ColumnProjector
//  {
//
//  }

  // TODO: Works only for direct column expressions, not for general
  // expressions. For general expressions, we'll need to extend the expression system.
  // TODO: Change to be type-specific to be more efficient for frames.
  protected static class OffsetProjector extends AbstractColumnProjector
  {
    private final PartitionReader reader;
    private final ScalarColumnReader sourceColumn;

    protected OffsetProjector(
        final ScalarColumnWriter destColumn,
        final ScalarColumnReader sourceColumn,
        final PartitionReader reader
    )
    {
      super(destColumn);
      this.reader = reader;
      this.sourceColumn = sourceColumn;
    }

    @Override
    public void project()
    {
      if (reader.isNull()) {
        destColumn.setNull();
      } else {
        destColumn.setValue(sourceColumn.getValue());
      }
    }
  }

//  public static class ExprProjector implements ColumnProjector
//  {
//
//  }

  private final RowCursor inputCursor;
  private final List<PartitionReader> readers;
  private final BatchWriter output;
  private final ColumnProjector[] columnProjections;

  public Projector(
      final RowCursor inputCursor,
      final List<PartitionReader> readers,
      final BatchWriter output,
      final ColumnProjector[] columnProjections
  )
  {
    this.inputCursor = inputCursor;
    this.readers = readers;
    this.output = output;
    this.columnProjections = columnProjections;
  }

  public void bindToPartition(PartitionRange range)
  {
    for (PartitionReader reader : readers) {
      reader.bind(range);
    }
  }

  public boolean isBatchFull()
  {
    return output.isFull();
  }

  public boolean project()
  {
    if (!inputCursor.next()) {
      return false;
    }
    output.newRow();
    for (int i = 0; i < columnProjections.length; i++) {
      columnProjections[i].project();
    }
    return true;
  }
}
