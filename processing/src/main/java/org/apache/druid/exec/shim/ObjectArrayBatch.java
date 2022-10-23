package org.apache.druid.exec.shim;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;

import java.util.List;

public class ObjectArrayBatch implements Batch
{
  private final RowSchema schema;
  private final List<Object[]> batch;

  public ObjectArrayBatch(final RowSchema schema, final List<Object[]> batch)
  {
    this.schema = schema;
    this.batch = batch;
  }

  @Override
  public BatchCapabilities capabilities()
  {
    return BatchCapabilities.IN_MEMORY_BATCH;
  }

  @Override
  public BatchReader newReader()
  {
    ObjectArrayListReader reader = new ObjectArrayListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof ObjectArrayListReader)) {
      return newReader();
    }
    ((ObjectArrayListReader) reader).bind(batch);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return new ObjectArrayListWriter(schema);
  }
}
