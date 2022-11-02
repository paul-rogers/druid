package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchCursor.BindableRowPositioner;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.AbstractBatchType;

import java.util.List;

/**
 * Batch that represents a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class ObjectArrayListBatchType extends AbstractBatchType
{
  public static final ObjectArrayListBatchType INSTANCE = new ObjectArrayListBatchType();

  public ObjectArrayListBatchType()
  {
    super(
        BatchType.BatchFormat.OBJECT_ARRAY,
        true,  // Can seek
        false, // Can't sort
        true   // Can write
    );
  }

  @Override
  public BatchCursor newCursor(RowSchema schema, BindableRowPositioner positioner)
  {
    return new ObjectArrayListCursor(schema, positioner);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return new ObjectArrayListWriter(schema, sizeLimit);
  }

  @Override
  public void bindCursor(BatchCursor cursor, Object data)
  {
    ((ObjectArrayListCursor) cursor).bind(cast(data));
  }

  @SuppressWarnings("unchecked")
  private List<Object[]> cast(Object data)
  {
    return (List<Object[]>) data;
  }

  @Override
  public int sizeOf(Object data)
  {
    return data == null ? 0 : cast(data).size();
  }

}
