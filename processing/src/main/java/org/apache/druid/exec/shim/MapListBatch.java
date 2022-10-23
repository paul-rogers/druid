package org.apache.druid.exec.shim;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;

import java.util.List;
import java.util.Map;

public class MapListBatch implements Batch
{
  private final RowSchema schema;
  private final List<Map<String, Object>> batch;

  public MapListBatch(final RowSchema schema, final List<Map<String, Object>> batch)
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
    MapListReader reader = new MapListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof MapListReader)) {
      return newReader();
    }
    ((MapListReader) reader).bind(batch);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return new MapListWriter(schema);
  }
}
