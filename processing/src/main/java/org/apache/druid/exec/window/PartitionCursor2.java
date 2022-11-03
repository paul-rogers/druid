package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.batch.RowSequencer;

public class PartitionCursor2 implements RowCursor
{
  private final BatchCursor baseCursor;

  @Override
  public ColumnReaderProvider columns()
  {
    return baseCursor.columns();
  }

  @Override
  public RowSequencer sequencer()
  {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BatchSchema schema()
  {
    return baseCursor.schema();
  }
}
