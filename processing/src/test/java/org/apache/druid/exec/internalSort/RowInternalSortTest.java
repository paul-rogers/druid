package org.apache.druid.exec.internalSort;

import org.apache.druid.exec.factory.FragmentFactory;
import org.apache.druid.exec.factory.OperatorConverter;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.fragment.Fragments;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.operator.Batches;
import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.ResultIterator.EofException;
import org.apache.druid.exec.operator.ResultIterator.StallException;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.exec.plan.InternalSortOp.SortType;
import org.apache.druid.exec.test.BatchBuilder;
import org.apache.druid.exec.test.SimpleDataGenFactory;
import org.apache.druid.exec.test.SimpleDataGenSpec;
import org.apache.druid.exec.util.BatchValidator;
import org.apache.druid.exec.util.BatchVisualizer;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

public class RowInternalSortTest
{
  private final OperatorConverter CONVERTER = new OperatorConverter(
      Collections.singletonList(new SimpleDataGenFactory())
  );

  @Test
  public void testEmptyInput()
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Collections.singletonList("rid"),
        BatchFormat.OBJECT_ARRAY,
        5,
        0
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rid", false))
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();
    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testSingleBatchSingleKey() throws StallException
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        10,
        8
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", false))
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();

    Batch actual = iter.next();
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(expected, actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testSingleBatchSingleKeyDesc() throws StallException
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        10,
        8
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", true))
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();

    Batch actual = iter.next();
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(Batches.reverseOf(expected), actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testMultipleBatchesSingleKey() throws StallException
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rid", "rand"),
        BatchFormat.OBJECT_ARRAY,
        3,
        8
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Collections.singletonList(new SortColumn("rand", false))
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();

    Batch actual = iter.next();
    RowSchema expectedSchema = new SchemaBuilder()
        .scalar("rid", ColumnType.LONG)
        .scalar("rand", ColumnType.LONG)
        .build();
    Batch expected = BatchBuilder.arrayList(expectedSchema)
        .row(1, 1)
        .row(6, 1)
        .row(2, 2)
        .row(7, 2)
        .row(3, 3)
        .row(8, 3)
        .row(4, 4)
        .row(5, 5)
        .build();
    BatchValidator.assertEquals(expected, actual);

    assertThrows(EofException.class, () -> iter.next());
  }

  @Test
  public void testMultipleKeysAndBatches() throws StallException
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rand", "str5"),
        BatchFormat.OBJECT_ARRAY,
        10,
        100
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Arrays.asList(
            new SortColumn("rand", false),
            new SortColumn("str5", true)
        )
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();

    Batch actual = iter.next();
    assertEquals(100, actual.size());
    BatchReader reader = actual.newReader();
    ScalarColumnReader randReader = reader.columns().scalar("rand");
    ScalarColumnReader str5Reader = reader.columns().scalar("str5");
    long lastRand = -1;
    String lastStr5 = "z";
    while (reader.cursor().next()) {
      long rand = randReader.getLong();
      assertTrue(rand >= lastRand);
      String str5 = str5Reader.getString();
      if (rand == lastRand) {
        assertTrue(str5.compareTo(lastStr5) <= 0);
      }
      lastRand = rand;
      lastStr5 = str5;
    }
  }

  @Test
  public void testLargeResultSet() throws StallException
  {
    SimpleDataGenSpec readerSpec = new SimpleDataGenSpec(
        1,
        Arrays.asList("rand", "str5"),
        BatchFormat.OBJECT_ARRAY,
        100,
        10_000
    );
    InternalSortOp sortSpec = new InternalSortOp(
        2,
        readerSpec,
        SortType.ROW,
        Arrays.asList(
            new SortColumn("rand", false),
            new SortColumn("str5", true)
        )
    );
    FragmentManager context = Fragments.defaultFragment();
    Operator root = CONVERTER.createTree(context, sortSpec);
    ResultIterator iter = root.open();

    Batch actual = iter.next();
    assertEquals(10_000, actual.size());
    BatchReader reader = actual.newReader();
    ScalarColumnReader randReader = reader.columns().scalar("rand");
    ScalarColumnReader str5Reader = reader.columns().scalar("str5");
    long lastRand = -1;
    String lastStr5 = "z";
    while (reader.cursor().next()) {
      long rand = randReader.getLong();
      assertTrue(rand >= lastRand);
      String str5 = str5Reader.getString();
      if (rand == lastRand) {
        assertTrue(str5.compareTo(lastStr5) <= 0);
      }
      lastRand = rand;
      lastStr5 = str5;
    }
  }
}
