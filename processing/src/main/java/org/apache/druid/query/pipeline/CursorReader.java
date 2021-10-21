package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.java.util.common.guava.BaseSequence;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.pipeline.Operator.AbstractOperatorDefn;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.BaseObjectColumnValueSelector;
import org.apache.druid.segment.Cursor;
import org.apache.druid.segment.Segment;
import org.apache.druid.segment.StorageAdapter;
import org.apache.druid.segment.VirtualColumn;
import org.apache.druid.segment.VirtualColumns;
import org.apache.druid.segment.column.ColumnHolder;
import org.apache.druid.segment.filter.Filters;
import org.joda.time.Interval;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * The cursor reader is a leaf operator which uses a cursor to access
 * data, which is returned via the operator protocol. Druid supports
 * a number of formats implemented by specific subclasses.
 */
// TODO: offset from response context
// TODO: Timeout from response context
public class CursorReader implements Operator
{
  static final String LEGACY_TIMESTAMP_KEY = "timestamp";

  public static class CursorReaderDefn extends AbstractOperatorDefn
  {
    public Segment segment;
    public ScanQuery.ResultFormat resultFormat;
    public List<String> columns;
    public VirtualColumns virtualColumns;
    public Interval interval;
    public Filter filter;
    public int batchSize;
    public int limit;
    public boolean descendingOrder;
    public boolean legacy;

    public boolean isWildcard()
    {
      return columns == null;
    }
  }

  public static class CursorReaderPlanner
  {
    public CursorReaderDefn plan(final ScanQuery query, final Segment segment)
    {
      CursorReaderDefn defn = new CursorReaderDefn();
      defn.segment = segment;
      defn.legacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");
      defn.columns = defineColumns(query);
      defn.virtualColumns = query.getVirtualColumns();
      List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
      Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);
      defn.interval = intervals.get(0);
      defn.filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
      defn.descendingOrder = query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
          (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending());
      return defn;
    }

    private List<String> defineColumns(ScanQuery query) {
      List<String> queryCols = query.getColumns();

      // Missing or empty list means wildcard
      if (queryCols == null || queryCols.isEmpty()) {
        return null;
      }
      final List<String> planCols = new ArrayList<>();
      if (query.isLegacy() && !queryCols.contains(LEGACY_TIMESTAMP_KEY)) {
        planCols.add(LEGACY_TIMESTAMP_KEY);
      }

      // Unless we're in legacy mode, allColumns equals query.getColumns() exactly. This is nice since it makes
      // the compactedList form easier to use.
      planCols.addAll(queryCols);
      return planCols;
    }

    public boolean isWildcard(ScanQuery query) {
      return (query.getColumns() == null || query.getColumns().isEmpty());
    }
  }

  public static class CursorReaderFactory implements OperatorFactory
  {
    @Override
    public Operator build(OperatorDefn opDefn, List<Operator> children) {
      Preconditions.checkArgument(children.isEmpty());
      CursorReaderDefn defn = (CursorReaderDefn) opDefn;
      return new CursorReader(defn);
    }
  }

  public static class CursorIterator
  {
    public enum State
    {
      START, RUN, DONE
    }
    private Yielder<Cursor> yielder;
    private State state = State.START;

    public CursorIterator(Sequence<Cursor> cursors) {
       this.yielder = cursors.toYielder(null,
           new YieldingAccumulator<Cursor, Cursor>()
           {
              @Override
              public Cursor accumulate(Cursor accumulated, Cursor in) {
                yield();
                return in;
              }
           }
      );
    }

    public boolean next()
    {
      switch (state) {
      case START:
        state = State.RUN;
        break;
      case RUN:
        yielder = yielder.next(null);
        break;
      case DONE:
        return false;
      }
      if (yielder.isDone()) {
        state = State.DONE;
        return false;
      }
      return true;
    }

    public Cursor get() {
      return yielder.get();
    }
  }

  public static abstract class BatchReader
  {
    protected final CursorReaderDefn defn;
    protected final List<String> allColumns;
    protected List<BaseObjectColumnValueSelector<?>> columnSelectors;
    protected long offset;

    public BatchReader(
        CursorReaderDefn defn,
        List<String> allColumns
        )
    {
      this.defn = defn;
      this.allColumns = allColumns;
    }

    public void startCursor(Cursor cursor) {
      columnSelectors = new ArrayList<>(allColumns.size());

      for (String column : allColumns) {
        final BaseObjectColumnValueSelector<?> selector;

        if (defn.legacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
          selector = cursor.getColumnSelectorFactory()
                           .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
        } else {
          selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
        }

        columnSelectors.add(selector);
      }
    }

    public abstract Object read(Cursor cursor);

    protected Object getColumnValue(int i)
    {
      final BaseObjectColumnValueSelector<?> selector = columnSelectors.get(i);
      final Object value;

      // TODO: optimize this to avoid compare on each get
      if (defn.legacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
        value = DateTimes.utc((long) selector.getObject());
      } else {
        value = selector == null ? null : selector.getObject();
      }

      return value;
    }
  }

  public static class ListBatchReader extends BatchReader
  {
    public ListBatchReader(
        CursorReaderDefn defn,
        List<String> allColumns
        )
    {
      super(defn, allColumns);
    }

    @Override
    public Object read(Cursor cursor) {
      List<Map<String, Object>> events = Lists.newArrayListWithCapacity(defn.batchSize);
      final long iterLimit = Math.min(defn.limit, offset + defn.batchSize);
      for (; !cursor.isDone() && offset < iterLimit; cursor.advance(), offset++) {
        final Map<String, Object> theEvent = new LinkedHashMap<>();
        for (int j = 0; j < allColumns.size(); j++) {
          theEvent.put(allColumns.get(j), getColumnValue(j));
        }
        events.add(theEvent);
      }
      return events;
    }
  }

  public static class BatchIterator
  {
    public enum State
    {
      CURSOR, BATCH, DONE
    }
    private final CursorIterator iter;
    private final BatchReader batchReader;
    private State state = State.CURSOR;

    public BatchIterator(CursorIterator iter, BatchReader batchReader)
    {
      this.iter = iter;
      this.batchReader = batchReader;
    }

    public boolean next() {
      if (state == State.BATCH && iter.get().isDone()) {
        batchReader.startCursor(iter.get());
        state = State.CURSOR;
      }
      switch (state) {
      case CURSOR:
        if (!iter.next()) {
          state = State.DONE;
          return false;
        }
        return true;
      case BATCH:
        return true;
      default:
        return false;
      }
    }

    public Object get() {
      return batchReader.read(iter.get());
    }
  }

  protected final CursorReaderDefn defn;
  protected BatchIterator batchIter;

  public CursorReader(CursorReaderDefn defn)
  {
    this.defn = defn;
  }

  @Override
  public void start() {
    final StorageAdapter adapter = defn.segment.asStorageAdapter();
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
    final List<String> cols;
    if (defn.isWildcard()) {
      cols = inferColumns(adapter);
    } else {
      cols = defn.columns;
    }
    final BatchReader batchReader;
    switch (defn.resultFormat) {
    case RESULT_FORMAT_LIST:
      batchReader = new ListBatchReader(defn, cols);
      break;
    default:
      throw new ISE("Unknown type");
    }
    Sequence<Cursor> cursors = adapter.makeCursors(
            defn.filter,
            defn.interval,
            defn.virtualColumns,
            Granularities.ALL,
            defn.descendingOrder,
            null
        );
    batchIter = new BatchIterator(
        new CursorIterator(cursors),
        batchReader
        );
  }

  protected List<String> inferColumns(StorageAdapter adapter)
  {
    List<String> cols = new ArrayList<>();
    final Set<String> availableColumns = Sets.newLinkedHashSet(
        Iterables.concat(
            Collections.singleton(defn.legacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
            Iterables.transform(
                Arrays.asList(defn.virtualColumns.getVirtualColumns()),
                VirtualColumn::getOutputName
            ),
            adapter.getAvailableDimensions(),
            adapter.getAvailableMetrics()
        )
    );

    cols.addAll(availableColumns);

    if (defn.legacy) {
      cols.remove(ColumnHolder.TIME_COLUMN_NAME);
    }
    return cols;
  }

  @Override
  public boolean next()
  {
    return batchIter.next();
  }

  @Override
  public Object get()
  {
    return batchIter.get();
  }

  @Override
  public void close()
  {
  }
}
