package org.apache.druid.exec.util;

import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.ColumnWriterFactory.ScalarColumnWriter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.segment.column.ColumnType;

public class ColumnCopierFactory
{
  private static abstract class AbstractCopier implements ColumnCopier
  {
    protected final ScalarColumnReader source;
    protected final ScalarColumnWriter dest;

    public AbstractCopier(ScalarColumnReader source, ScalarColumnWriter dest)
    {
       this.source = source;
      this.dest = dest;
    }
  }

  public static ColumnCopier build(ScalarColumnReader source, ScalarColumnWriter dest)
  {
    ColumnType sourceType = source.schema().type();
    ColumnType destType = source.schema().type();
    if (sourceType == ColumnType.UNKNOWN_COMPLEX || destType == ColumnType.UNKNOWN_COMPLEX) {
      if (sourceType != ColumnType.UNKNOWN_COMPLEX || destType != ColumnType.UNKNOWN_COMPLEX) {
        throw new UOE(
            "Can only copy complex values to other complex values: [%s] -> [%s]",
            sourceType,
            destType
        );
      }
      return new AbstractCopier(source, dest) {
        @Override
        public void copy()
        {
          dest.setObject(source.getObject());
        }
      };
    }
    if (sourceType != destType) {

      // Don't know the type, or the types differ. Have to go the slow route
      // to parse each value.
      return new AbstractCopier(source, dest) {
        @Override
        public void copy()
        {
          dest.setValue(source.getValue());
        }
      };
    }
    if (sourceType == ColumnType.STRING) {
      return new AbstractCopier(source, dest) {
        @Override
        public void copy()
        {
          dest.setString(source.getString());
        }
      };
    }
    if (sourceType == ColumnType.LONG) {
      return new AbstractCopier(source, dest) {
        @Override
        public void copy()
        {
          dest.setLong(source.getLong());
        }
      };
    }
    if (sourceType == ColumnType.FLOAT || sourceType == ColumnType.DOUBLE) {
      return new AbstractCopier(source, dest) {
        @Override
        public void copy()
        {
          dest.setDouble(source.getDouble());
        }
      };
    }

    // Type not yet supported here. For now, just copy generically.
    // If we hit this line, it means we need additional implementations.
    return new AbstractCopier(source, dest) {
      @Override
      public void copy()
      {
        dest.setValue(source.getValue());
      }
    };
  }
}
