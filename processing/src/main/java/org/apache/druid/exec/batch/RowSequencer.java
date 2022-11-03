package org.apache.druid.exec.batch;

/**
 * Read a set of rows sequentially. Call {@link #next()}
 * to move to the first row. The cursor starts before the position before the first row,
 * and ends beyond the position of the last row.
 */
public interface RowSequencer
{
  /**
   * Move to the next row, if any. At EOF, the position points past the
   * last row.
   *
   * @return {@code true} if there is a row to read, {@code false} if EOF.
   */
  boolean next();

  /**
   * Report if the reader is positioned past the end of the last row (i.e. EOF).
   * This call is not normally needed: the call to {@ink #next()} reports the
   * same information.
   */
  boolean isEOF();

  /**
   * Report if the reader is positioned on a valid data row.
   *
   * @return {true} if a data row is available, {@false} if the cursor is
   * positioned before the first or after the last row, and so no row is
   * available.
   */
  boolean isValid();
}