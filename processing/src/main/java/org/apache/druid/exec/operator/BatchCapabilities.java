package org.apache.druid.exec.operator;

/**
 * Describes the physical characteristics of an operator branch. Operators
 * should be written to assume a simple one-pass iteration over the batch
 * using the column readers (selectors) provided. Advanced use cases
 * (such as a sort), might optimize based on batch capabilities.
 */
public interface BatchCapabilities
{
  enum BatchFormat
  {
    OBJECT_ARRAY,
    MAP,
    SCAN_OBJECT_ARRAY,
    SCAN_MAP
  }

  boolean canSeek();
  boolean canSort();
  BatchFormat format();
  boolean canDirectCopyFrom(BatchFormat format);
}
