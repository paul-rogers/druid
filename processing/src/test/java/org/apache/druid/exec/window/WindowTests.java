package org.apache.druid.exec.window;

import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.RowWriter;
import org.apache.druid.exec.batch.RowCursor;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.test.SimpleDataGenOperator;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.test.TestUtils;

import java.util.Arrays;

public class WindowTests
{
  public static SimpleDataGenSpec dataGenSpec(int batchSize, int rowCount)
  {
    return new SimpleDataGenSpec(
        1,
        Arrays.asList("rid"),
        BatchFormat.OBJECT_ARRAY,
        batchSize,
        rowCount
    );
  }

  public static BatchOperator dataGen(int batchSize, int rowCount)
  {
    return new SimpleDataGenOperator(
        TestUtils.emptyFragment(),
        dataGenSpec(batchSize, rowCount)
    );
  }

  public static Batch copyReader(RowCursor cursor)
  {
    BatchWriter<?> writer = cursor.schema().newWriter(Integer.MAX_VALUE);
    RowWriter rowWriter = writer.rowWriter(cursor.columns().columns());
    while (cursor.next()) {
      rowWriter.write();
    }
    return writer.harvestAsBatch();
  }


}
