package org.apache.druid.exec.batch;

/**
 * Reader for a possibly unbounded set of rows with a common schema and common
 * set of column readers, where "unbounded" simply means that the total number
 * of rows is not necessarily known. Reading is done using the column readers.
 * The result isolates the client from the implementation details of the underlying
 * data set which may be materialized, streaming, or some combination.
 * <p>
 * A reader has an implicit read position defined by the implementation.
 * The most common use is as part of a {@link RowCursor} which also provides
 * an indexing interface.
 */
public interface RowReader
{
  BatchSchema schema();
  ColumnReaderProvider columns();
}
