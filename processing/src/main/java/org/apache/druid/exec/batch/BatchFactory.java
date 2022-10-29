package org.apache.druid.exec.batch;

public class BatchFactory
{
  private final BatchType batchType;
  private final RowSchema schema;

  public BatchFactory(BatchType batchType, RowSchema schema)
  {
    this.batchType = batchType;
    this.schema = schema;
  }

  public BatchType type()
  {
    return batchType;
  }

  public RowSchema schema()
  {
    return schema;
  }

  public Batch newBatch()
  {
    return batchType.newBatch(schema);
  }

  public Batch of(Object data)
  {
    Batch batch = newBatch();
    batch.bind(data);
    return batch;
  }

  public BatchWriter<?> newWriter(int sizeLimit)
  {
    return batchType.newWriter(schema, sizeLimit);
  }

  public BatchReader newReader()
  {
    return batchType.newReader(schema);
  }
}
