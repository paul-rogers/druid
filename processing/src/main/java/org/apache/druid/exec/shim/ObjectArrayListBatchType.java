package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.AbstractBatchType;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;

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
        true, // Can seek
        false // Can't sort
    );
  }

  @Override
  public BatchReader newReader(RowSchema schema)
  {
    return new ObjectArrayListReader(schema);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return new ObjectArrayListWriter(schema, sizeLimit);
  }

  @Override
  public void bindReader(BatchReader reader, Object data)
  {
    ((ObjectArrayListReader) reader).bind(cast(data));
  }

  @SuppressWarnings("unchecked")
  private List<Object[]> cast(Object data)
  {
    return (List<Object[]>) data;
  }

  @Override
  public int sizeOf(Object data)
  {
    return cast(data).size();
  }

}
