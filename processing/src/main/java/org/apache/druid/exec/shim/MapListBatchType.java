package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.AbstractBatchType;

import java.util.List;
import java.util.Map;

public class MapListBatchType extends AbstractBatchType
{
  public static final MapListBatchType INSTANCE = new MapListBatchType();

  public MapListBatchType()
  {
    super(
        BatchType.BatchFormat.MAP,
        true,  // Can seek
        false, // Can't sort
        true   // Can write
    );
  }

  @Override
  public BatchReader newReader(RowSchema schema)
  {
    return new MapListReader(schema);
  }

  @Override
  public BatchWriter<List<Map<String, Object>>> newWriter(RowSchema schema, int sizeLimit)
  {
    return new MapListWriter(schema, sizeLimit);
  }

  @Override
  public void bindReader(BatchReader reader, Object data)
  {
    ((MapListReader) reader).bind(cast(data));
  }

  @SuppressWarnings("unchecked")
  private List<Map<String, Object>> cast(Object data)
  {
    return (List<Map<String, Object>>) data;
  }

  @Override
  public int sizeOf(Object data)
  {
    return data == null ? 0 : cast(data).size();
  }
}
