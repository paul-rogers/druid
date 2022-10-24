package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.impl.BatchCapabilitiesImpl;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;

import java.util.List;

public class ObjectArrayBatch implements Batch
{
  public static final BatchCapabilities CAPABILITIES = new BatchCapabilitiesImpl(
      BatchFormat.OBJECT_ARRAY,
      true, // Can seek
      false // Can't sort
  );

  private final RowSchema schema;
  private final List<Object[]> batch;

  public ObjectArrayBatch(final RowSchema schema, final List<Object[]> batch)
  {
    this.schema = schema;
    this.batch = batch;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public int size()
  {
    return batch.size();
  }

  @Override
  public BatchCapabilities capabilities()
  {
    return CAPABILITIES;
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
