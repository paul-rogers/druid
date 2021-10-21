package org.apache.druid.query.pipeline;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.druid.java.util.common.DateTimes;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.filter.Filter;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
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

  /**
   * Static definition of a segment scan.
   */
  public static class CursorReaderDefn extends AbstractOperatorDefn
  {
    public static enum Limit
    {
      NONE,
      /**
       * If we're performing time-ordering, we want to scan through the first `limit` rows in each
       * segment ignoring the number of rows already counted on other segments.
       */
      LOCAL,
      GLOBAL
    }
    public Segment segment;
    public ScanQuery.ResultFormat resultFormat;
    public List<String> columns;
    public VirtualColumns virtualColumns;
    public Interval interval;
    public Filter filter;
    public int batchSize;
    public Limit limitType;
    public long limit;
    public boolean descendingOrder;
    public boolean legacy;
    public boolean hasTimeout;

    public boolean isWildcard()
    {
      return columns == null;
    }
  }

  /**
   * Create a segment scan definition from a scan query.
   */
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
      defn.hasTimeout = QueryContexts.hasTimeout(query);
      defn.limit = query.getScanRowsLimit();
      if (!query.isLimited()) {
        defn.limitType = CursorReaderDefn.Limit.NONE;
      } else if (query.getOrder().equals(ScanQuery.Order.NONE)) {
        defn.limitType = CursorReaderDefn.Limit.LOCAL;
      } else {
        defn.limitType = CursorReaderDefn.Limit.GLOBAL;
      }
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

  /**
   * Create a cursor reader from a definition.
   */
  public static class CursorReaderFactory implements OperatorFactory
  {
    @Override
    public Operator build(OperatorDefn opDefn, List<Operator> children, FragmentContext context) {
      Preconditions.checkArgument(children.isEmpty());
      CursorReaderDefn defn = (CursorReaderDefn) opDefn;
      return new CursorReader(defn, context);
    }
  }

  /**
   * Read events from a cursor into one of several row formats.
   */
  public static abstract class BatchReader
  {
    protected final CursorReaderDefn defn;
    protected final List<String> allColumns;
    private final long limit;
    private List<BaseObjectColumnValueSelector<?>> columnSelectors;
    protected long rowCount;
    private Cursor cursor;
    private long targetCount;

    public BatchReader(
        CursorReaderDefn defn,
        List<String> allColumns,
        long limit
        )
    {
      this.defn = defn;
      this.allColumns = allColumns;
      this.limit = limit;
    }

    public void startCursor(Cursor cursor) {
      this.cursor = cursor;
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

    public abstract Object read();

    protected void startBatch() {
      targetCount = Math.min(limit - rowCount, rowCount + defn.batchSize);
    }

    protected boolean atLimit()
    {
      return rowCount >= limit;
    }

    protected boolean hasNextBatch()
    {
      return !cursor.isDone() && !atLimit();
    }

    protected boolean hasNextRow()
    {
      return !cursor.isDone() && rowCount < targetCount;
    }

    protected void advance()
    {
      cursor.advance();
      rowCount++;
     }

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

  /**
   * Convert a cursor row into a simple list of maps, where each map
   * represents a single event, and each map entry represents a column.
   */
  public static class ListOfMapBatchReader extends BatchReader
  {
    public ListOfMapBatchReader(
        CursorReaderDefn defn,
        List<String> allColumns,
        long limit
        )
    {
      super(defn, allColumns, limit);
    }

    @Override
    public Object read() {
      startBatch();
      List<Map<String, Object>> events = Lists.newArrayListWithCapacity(defn.batchSize);
      while (hasNextRow()) {
        final Map<String, Object> theEvent = new LinkedHashMap<>();
        for (int j = 0; j < allColumns.size(); j++) {
          theEvent.put(allColumns.get(j), getColumnValue(j));
        }
        events.add(theEvent);
        advance();
      }
      return events;
    }
  }

  private enum State
  {
    CURSOR, BATCH, DONE
  }
  protected final CursorReaderDefn defn;
  protected final ResponseContext responseContext;
  private SequenceIterator<Cursor> iter;
  private BatchReader batchReader;
  private State state = null;
  private long timeoutAt;
  private long startTime;

  public CursorReader(CursorReaderDefn defn, FragmentContext context)
  {
    this.defn = defn;
    this.responseContext = context.responseContext();
  }

  @Override
  public void start() {
    timeoutAt = (long) responseContext.get(ResponseContext.Key.TIMEOUT_AT);
    startTime = System.currentTimeMillis();
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
    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, 0L);
    long limit = defn.limit;
    if (defn.limitType == CursorReaderDefn.Limit.GLOBAL) {
      limit -= (Long) responseContext.get(ResponseContext.Key.NUM_SCANNED_ROWS);
    }
    switch (defn.resultFormat) {
    case RESULT_FORMAT_LIST:
      batchReader = new ListOfMapBatchReader(defn, cols, limit);
      break;
    default:
      throw new ISE("Unknown type");
    }
    iter = SequenceIterator.of(adapter.makeCursors(
            defn.filter,
            defn.interval,
            defn.virtualColumns,
            Granularities.ALL,
            defn.descendingOrder,
            null
        ));
    state = State.CURSOR;
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
  public boolean hasNext()
  {
    Preconditions.checkState(state != null, "start() not called");
    while (true) {
      switch (state) {
      case CURSOR:
        if (!iter.hasNext()) {
          finish();
          return false;
        }
        batchReader.startCursor(iter.next());
        state = State.BATCH;
        break;
      case BATCH:
        if (batchReader.hasNextBatch()) {
          return true;
        }
        if (batchReader.atLimit()) {
          finish();
          return false;
        }
        state = State.CURSOR;
        break;
      default:
        return false;
      }
    }
  }

  private void finish()
  {
    state = State.DONE;
    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, batchReader.rowCount);
    if (defn.hasTimeout) {
      // This is very likely wrong
      responseContext.put(
          ResponseContext.Key.TIMEOUT_AT,
          timeoutAt - (System.currentTimeMillis() - startTime)
      );
    }
  }

  @Override
  public Object next()
  {
    Preconditions.checkState(state == State.BATCH);
    return new ScanResultValue(
        defn.segment.getId().toString(),
        batchReader.allColumns,
        batchReader.read());
  }

  @Override
  public void close()
  {
    state = State.DONE;
  }
}
