package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.BatchReader;

public class BatchImpl implements Batch
{
  private final BatchFactory factory;
  private Object data;

  public BatchImpl(BatchFactory factory)
  {
    this.factory = factory;
  }

  public BatchImpl(BatchFactory factory, Object data)
  {
    this.factory = factory;
    this.data = data;
  }

  @Override
  public BatchFactory factory()
  {
    return factory;
  }

  @Override
  public void bind(Object data)
  {
    this.data = data;
  }

  @Override
  public BatchReader newReader()
  {
    BatchReader reader = factory.newReader();
    bindReader(reader);
    return reader;
  }

  @Override
  public void bindReader(BatchReader reader)
  {
    factory.type().bindReader(reader, data);
  }

  @Override
  public int size()
  {
    return data == null ? 0 : factory.type().sizeOf(data);
  }

  @Override
  public Object data()
  {
    return data;
  }
}
