package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.BatchCursor.BindableRowPositioner;
import org.apache.druid.exec.batch.impl.AbstractBatchType;
import org.apache.druid.java.util.common.UOE;

/**
 * Batch that represents a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class SingletonObjectArrayBatchType extends AbstractBatchType
{
  public static final SingletonObjectArrayBatchType INSTANCE = new SingletonObjectArrayBatchType();

  public SingletonObjectArrayBatchType()
  {
    super(
        BatchType.BatchFormat.OBJECT_ARRAY,
        false, // Can't seek
        false, // Can't sort
        false  // Can't write
    );
  }

  @Override
  public BatchCursor newCursor(RowSchema schema, BindableRowPositioner positioner)
  {
    throw new UOE("Can't provide a positioner for the singleton reader");
  }

  @Override
  public BatchCursor newCursor(RowSchema schema)
  {
    return new SingletonObjectArrayCursor(schema);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return null;
  }

  @Override
  public void bindCursor(BatchCursor cursor, Object data)
  {
    ((SingletonObjectArrayCursor) cursor).bind((Object[]) data);
  }

  @Override
  public int sizeOf(Object data)
  {
    return data == null ? 0 : 1;
  }

  @Override
  public boolean canDirectCopyFrom(BatchType otherType)
  {
    return false;
  }
}
