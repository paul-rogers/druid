package org.apache.druid.exec.test;

import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.shim.MapListWriter;
import org.apache.druid.exec.shim.ObjectArrayListWriter;
import org.apache.druid.exec.shim.ScanResultValueWriter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;

public class TestUtils
{
  public static BatchBuilder builderFor(RowSchema schema, BatchFormat format)
  {
    return BatchBuilder.of(writerFor(schema, format, Integer.MAX_VALUE));
  }

  public static BatchWriter writerFor(RowSchema schema, BatchFormat format, int limit)
  {
    switch (format) {
      case OBJECT_ARRAY:
        return new ObjectArrayListWriter(schema, limit);
      case MAP:
        return new MapListWriter(schema, limit);
      case SCAN_OBJECT_ARRAY:
        return new ScanResultValueWriter("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST, limit);
      case SCAN_MAP:
        return new ScanResultValueWriter("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_LIST, limit);
      default:
        throw new UOE("Invalid batch format");
    }
  }
}
