/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.exec.internalSort;

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import org.apache.druid.exec.batch.Batch;
import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchCursor.RowPositioner;
import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.BatchWriter.Copier;
import org.apache.druid.exec.batch.ColumnReaderProvider;
import org.apache.druid.exec.batch.ColumnReaderProvider.ScalarColumnReader;
import org.apache.druid.exec.batch.impl.IndirectBatchType;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.Iterators;
import org.apache.druid.exec.operator.Operators;
import org.apache.druid.exec.operator.ResultIterator;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.exec.util.TypeRegistry;
import org.apache.druid.frame.key.SortColumn;
import org.apache.druid.java.util.common.ISE;

import java.util.Comparator;
import java.util.List;

public class RowInternalSortOperator extends InternalSortOperator
{
  private static class RowComparator implements IntComparator
  {
    private final RowPositioner leftPositioner;
    private final RowPositioner rightPositioner;
    private final ScalarColumnReader[] leftCols;
    private final ScalarColumnReader[] rightCols;
    private final Comparator<Object>[] comparators;

    private RowComparator(Batch results, List<SortColumn> keys)
    {
      final BatchCursor cursor1 = results.newCursor();
      final BatchCursor cursor2 = results.newCursor();
      this.leftPositioner = cursor1.positioner();
      this.rightPositioner = cursor2.positioner();
      final ColumnReaderProvider leftColumns = cursor1.columns();
      final ColumnReaderProvider rightColumns = cursor2.columns();
      this.leftCols = new ScalarColumnReader[keys.size()];
      this.rightCols = new ScalarColumnReader[keys.size()];
      comparators = TypeRegistry.INSTANCE.sortOrdering(keys, leftColumns.schema());
      for (int i = 0; i < keys.size(); i++) {
        SortColumn key = keys.get(i);
        leftCols[i] = leftColumns.scalar(key.columnName());
        rightCols[i] = rightColumns.scalar(key.columnName());
        if (leftCols[i] == null) {
          throw new ISE("Sort key [%s] not found in the input schema", key.columnName());
        }
      }
    }

    @Override
    public int compare(int k1, int k2)
    {
      leftPositioner.seek(k1);
      rightPositioner.seek(k2);
      for (int i = 0; i < leftCols.length; i++) {
        int result = comparators[i].compare(leftCols[i].getValue(), rightCols[i].getValue());
        if (result != 0) {
          return result;
        }
      }
      return 0;
    }
  }

  public RowInternalSortOperator(FragmentContext context, InternalSortOp plan, BatchOperator input)
  {
    super(context, plan, input);
  }

  @Override
  protected ResultIterator<Object> doSort() throws EofException
  {
    return sortRows(loadInput());
  }

  private Batch loadInput() throws EofException
  {
    BatchSchema batchSchema = input.batchSchema();
    BatchType batchType = batchSchema.type();
    BatchCursor inputCursor = batchSchema.newCursor();
    // TODO: All in one array. Consider creating multiple runs and merging.
    BatchWriter<?> runWriter = batchSchema.newWriter(Integer.MAX_VALUE);
    Copier copier = runWriter.copier(inputCursor);
    runWriter.newBatch();
    copier.copy(Integer.MAX_VALUE);
    while (true) {
      try {
        batchType.bindCursor(inputCursor, inputIter.next());
        copier.copy(Integer.MAX_VALUE);
      }
      catch (EofException e) {
        break;
      }
    }
    rowCount = runWriter.size();
    return runWriter.harvestAsBatch();
  }

  private ResultIterator<Object> sortRows(Batch results) throws EofException
  {
    if (rowCount == 0) {
      throw Operators.eof();
    }
    int[] index = new int[rowCount];
    for (int i = 0; i < rowCount; i++) {
      index[i] = i;
    }
    IntArrays.quickSort(index, new RowComparator(results, keys));
    batchCount++;

    // Single result: the results and sorted indirection vector.
    return Iterators.singletonIterator(IndirectBatchType.wrap(results.data(), index));
  }
}
