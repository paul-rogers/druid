package org.apache.druid.exec.operator.impl;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.ColumnReaderFactory;
import org.apache.druid.exec.operator.ColumnWriterFactory;
import org.apache.druid.exec.shim.DelegatingBatchReader;
import org.apache.druid.exec.util.BatchCopier;
import org.apache.druid.exec.util.BatchCopierFactory;
import org.apache.druid.java.util.common.UOE;

public class Batches
{
  public static Batch indirectBatch(Batch results, int[] index)
  {
    return new IndirectBatch(results, index);
  }

  /**
   * Optimized copy of rows from one batch to another. To fully optimize, ensure
   * the same reader and writer are used across batches to avoid the need to
   */
  public static BatchCopier copier(BatchReader source, BatchWriter dest)
  {
    return BatchCopierFactory.build(source, dest);
  }

  /**
   * Convenience, non-optimized method to copy a all rows between batches
   * with compatible schemas. Consider {@link BatchCopier}, obtained from
   * {@link #copier(BatchReader, BatchWriter)}, for production use.
   */
  public static boolean copy(BatchReader source, BatchWriter dest)
  {
    while (source.cursor().next()) {
      if (!copyRow(source, dest)) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convenience, non-optimized method to copy a row between batches
   * with compatible schemas. Consider {@link org.apache.druid.exec.util.BatchCopier}
   * for production use.
   */
  private static boolean copyRow(BatchReader source, BatchWriter dest)
  {
    ColumnReaderFactory sourceColumns = source.columns();
    ColumnWriterFactory destColumns = dest.columns();
    int columnCount = sourceColumns.schema().size();

    // Quick & dirty check on the number of columns. We trust that
    // the caller has ensured the types match or are compatible.
    if (destColumns.schema().size() != columnCount) {
      throw new UOE("Cannot copy rows between differing schemas: use a projection");
    }
    for (int i = 0; i < columnCount; i++) {
      destColumns.scalar(i).setValue(sourceColumns.scalar(i).getValue());
    }
    return false;
  }

  public BatchReader unwrap(BatchReader reader)
  {
    if (reader instanceof DelegatingBatchReader) {
      return ()
    }
  }
}
