package org.apache.druid.exec.batch;

/**
 * Describes the physical characteristics of an operator branch. Operators
 * should be written to assume a simple one-pass iteration over the batch
 * using the column readers (selectors) provided. Advanced use cases
 * (such as a sort), might optimize based on batch capabilities.
 */
public interface BatchType
{
  enum BatchFormat
  {
    OBJECT_ARRAY,
    MAP,
    SCAN_OBJECT_ARRAY,
    SCAN_MAP,
    UNKNOWN
  }

  BatchFormat format();
  boolean canWrite();
  boolean canSeek();
  boolean canSort();
  BatchFactory factory(RowSchema schema);
  Batch newBatch(RowSchema schema);
  BatchReader newReader(RowSchema schema);
  BatchWriter<?> newWriter(RowSchema schema, int sizeLimit);
  void bindReader(BatchReader reader, Object data);
  int sizeOf(Object data);
  boolean canDirectCopyFrom(BatchType otherType);
}