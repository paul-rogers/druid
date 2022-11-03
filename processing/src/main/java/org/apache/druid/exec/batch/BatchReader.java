package org.apache.druid.exec.batch;

import org.apache.druid.exec.util.ExecUtils;

/**
 * Reader for a batch of rows.
 * <p>
 * Call {@link #bind(Object)} to attach the reader to the actual data batch. Calling
 * {@code bind()} does an implicit {@code reset()}.
 * <p>
 * Call {@link #row()} to obtain the row reader for a row. The row reader is the same
 * for all rows: the batch reader positions the row reader automatically. Client code
 * can cache the row reader, or fetch it each time it is needed.
 */
public interface BatchReader extends RowReader, PositionListener
{
  int size();
  void bindListener(BindingListener listener);

  default <T> T unwrap(Class<T> readerClass)
  {
    return ExecUtils.unwrap(this, readerClass);
  }
}
