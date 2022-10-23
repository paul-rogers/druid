package org.apache.druid.exec.internalSort;

import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchReader.BatchCursor;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.ColumnReaderFactory;
import org.apache.druid.exec.operator.ColumnReaderFactory.ScalarColumnReader;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.operator.impl.Batches;
import org.apache.druid.exec.operator.impl.Iterators;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.exec.util.BatchCopier;
import org.apache.druid.exec.util.OrderingBuilder;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.List;

public class RowInternalSortOperator extends InternalSortOperator
{
  private static class RowComparator implements IntComparator
  {
    private final BatchCursor leftCursor;
    private final BatchCursor rightCursor;
    private final ScalarColumnReader[] leftCols;
    private final ScalarColumnReader[] rightCols;
    private final Comparator<Object>[] comparators;

    private RowComparator(Batch results, List<SortColumn> keys)
    {
      final BatchReader reader1 = results.newReader();
      final BatchReader reader2 = results.newReader();
      this.leftCursor = reader1.cursor();
      this.rightCursor = reader2.cursor();
      final ColumnReaderFactory leftColumns = reader1.columns();
      final ColumnReaderFactory rightColumns = reader2.columns();
      this.leftCols = new ScalarColumnReader[keys.size()];
      this.rightCols = new ScalarColumnReader[keys.size()];
      this.comparators = makeComparators(keys.size());
      for (int i = 0; i < keys.size(); i++) {
        SortColumn key = keys.get(i);
        leftCols[i] = leftColumns.scalar(key.columnName());
        rightCols[i] = rightColumns.scalar(key.columnName());
        if (leftCols[i] == null) {
          throw new ISE("Sort key [%s] not found in the input schema", key.columnName());
        }
        comparators[i] = OrderingBuilder.sortOrdering(key, leftCols[i].schema().type());
      }
    }

    // Just to suppress the warning.
    @SuppressWarnings("unchecked")
    private static Comparator<Object>[] makeComparators(int n)
    {
      return new Comparator[n];
    }

    @Override
    public int compare(int k1, int k2)
    {
      leftCursor.seek(k1);
      rightCursor.seek(k2);
      for (int i = 0; i < leftCols.length; i++) {
        int result = comparators[i].compare(leftCols[i].getValue(), rightCols[i].getValue());
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }
  }

  public RowInternalSortOperator(FragmentContext context, InternalSortOp plan, List<Operator> children)
  {
    super(context, plan, children);
  }

  @Override
  protected ResultIterator doSort() throws StallException
  {
    Batch results = loadInput();
    return sortRows(results);
  }

  private Batch loadInput() throws StallException
  {
    openInput();
    Batch inputBatch = inputIter.next();
    BatchReader inputReader = inputBatch.newReader();
    BatchWriter runWriter = inputBatch.newWriter();
    BatchCopier copier = Batches.copier(inputReader, runWriter);
    copier.copyAll(inputReader, runWriter);
    while (true)
    {
      try {
        inputBatch = inputIter.next();
        BatchReader newReader = inputBatch.bindReader(inputReader);
        Preconditions.checkState(newReader == inputReader);
        copier.copyAll(inputReader, runWriter);
      }
      catch (EofException e) {
        break;
      }
    }
    rowCount = runWriter.size();
    return runWriter.harvest();
  }

  private ResultIterator sortRows(Batch results)
  {
    int[] index = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      index[i] = i;
    }
    IntArrays.quickSort(index, new RowComparator(results, keys));
    Batch sorted = Batches.indirectBatch(results, index);
    return Iterators.singletonIterator(sorted);
  }
}
