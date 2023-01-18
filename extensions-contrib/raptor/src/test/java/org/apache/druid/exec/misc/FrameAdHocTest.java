package org.apache.druid.exec.misc;

import org.apache.druid.frame.FrameType;
import org.apache.druid.frame.allocation.ArenaMemoryAllocator;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.frame.read.FrameReader;
import org.apache.druid.frame.write.FrameWriter;
import org.apache.druid.frame.write.FrameWriterFactory;
import org.apache.druid.frame.write.FrameWriters;
import org.apache.druid.frame.write.RowBasedFrameWriterFactory;
import org.apache.druid.query.dimension.DimensionSpec;
import org.apache.druid.query.monomorphicprocessing.RuntimeShapeInspector;
import org.apache.druid.segment.ColumnSelectorFactory;
import org.apache.druid.segment.ColumnValueSelector;
import org.apache.druid.segment.DimensionSelector;
import org.apache.druid.segment.column.ColumnCapabilities;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.hamcrest.CoreMatchers;
import org.hamcrest.MatcherAssert;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class FrameAdHocTest
{
  private static final int ALLOCATOR_CAPACITY = 1000;

  public static class DummySelectorFactory implements ColumnSelectorFactory
  {
    int posn;

    @Override
    public DimensionSelector makeDimensionSelector(DimensionSpec dimensionSpec)
    {
      return DimensionSelector.constant(null);
    }

    @Override
    public ColumnValueSelector<?> makeColumnValueSelector(String columnName)
    {
      if ("foo".equals(columnName))
      {
        return new ColumnValueSelector<String>()
        {
          @Override
          public long getLong()
          {
            return 0;
          }

          @Override
          public void inspectRuntimeShape(RuntimeShapeInspector inspector)
          {
          }

          @Override
          public boolean isNull()
          {
            return false;
          }

          @Override
          public double getDouble()
          {
            return 0;
          }

          @Override
          public float getFloat()
          {
            return 0;
          }

          @Override
          public Class<String> classOfObject()
          {
            return String.class;
          }

          @Override
          public String getObject()
          {
            return "row " + (posn + 1);
          }
        };
      }
      return null;
    }

    @Override
    public ColumnCapabilities getColumnCapabilities(String column)
    {
      return null;
    }
  }

  @Test
  public void test_rowBased()
  {
    RowSignature rowSig = RowSignature.builder().add("foo", ColumnType.STRING).build();
    final FrameWriterFactory factory = FrameWriters.makeFrameWriterFactory(
        FrameType.ROW_BASED,
        ArenaMemoryAllocator.createOnHeap(ALLOCATOR_CAPACITY),
        rowSig,
        Collections.singletonList(new SortColumn("x", false))
    );

    MatcherAssert.assertThat(factory, CoreMatchers.instanceOf(RowBasedFrameWriterFactory.class));
    Assert.assertEquals(ALLOCATOR_CAPACITY, factory.allocatorCapacity());

    DummySelectorFactory selFactory = new DummySelectorFactory();
    FrameWriter writer = factory.newFrameWriter(selFactory);

    writer.addSelection();
    selFactory.posn++;
    writer.addSelection();
    selFactory.posn++;
    writer.addSelection();
    writer.close();

    assertEquals(3, writer.getNumRows());

    FrameReader reader = FrameReader.create(rowSig);
    CursorFactory cursorFactory = reader.makeCursorFactory(writer);
    Sequence<Cursor> cursors = cursorFactory.make
  }


}
