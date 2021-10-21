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
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.granularity.Granularities;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.QueryTimeoutException;
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
    public final ScanQuery query;
    public final Segment segment;
    public final String segmentId;
    public final List<String> columns;
    public final Filter filter;
    public final boolean isLegacy;
    public final int batchSize;

    public CursorReaderDefn(final ScanQuery query, final Segment segment)
    {
      this.query = query;
      this.segment = segment;
      this.segmentId = segment.getId().toString();
      this.columns = defineColumns(query);
      List<Interval> intervals = query.getQuerySegmentSpec().getIntervals();
      Preconditions.checkArgument(intervals.size() == 1, "Can only handle a single interval, got[%s]", intervals);
      this.filter = Filters.convertToCNFFromQueryContext(query, Filters.toFilter(query.getFilter()));
      this.isLegacy = Preconditions.checkNotNull(query.isLegacy(), "Expected non-null 'legacy' parameter");
      this.batchSize = query.getBatchSize();
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

    public boolean isDescendingOrder()
    {
      return query.getOrder().equals(ScanQuery.Order.DESCENDING) ||
          (query.getOrder().equals(ScanQuery.Order.NONE) && query.isDescending());
    }

    public boolean hasTimeout()
    {
      return QueryContexts.hasTimeout(query);
    }

    public boolean isWildcard()
    {
      return columns == null;
    }

    public Limit limitType()
    {
      if (!query.isLimited()) {
        return CursorReaderDefn.Limit.NONE;
      } else if (query.getOrder().equals(ScanQuery.Order.NONE)) {
        return CursorReaderDefn.Limit.LOCAL;
      } else {
        return CursorReaderDefn.Limit.GLOBAL;
      }
    }

    public Interval interval()
    {
      return query.getQuerySegmentSpec().getIntervals().get(0);
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
  public interface BatchReader
  {
    Object read();
  }

  /**
   * Convert a cursor row into a simple list of maps, where each map
   * represents a single event, and each map entry represents a column.
   */
  public class ListOfMapBatchReader implements BatchReader
  {
    @Override
    public Object read() {
      final List<Map<String, Object>> events = new ArrayList<>(defn.batchSize);
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

  public class CompactListBatchReader implements BatchReader
  {
    @Override
    public Object read() {
      final List<List<Object>> events = new ArrayList<>(defn.batchSize);
      while (hasNextRow()) {
        final List<Object> theEvent = new ArrayList<>(allColumns.size());
        for (int j = 0; j < allColumns.size(); j++) {
          theEvent.add(getColumnValue(j));
        }
        events.add(theEvent);
        advance();
      }
      return events;
    }
  }

  protected final CursorReaderDefn defn;
  protected final ResponseContext responseContext;
  private SequenceIterator<Cursor> iter;
  private BatchReader batchReader;
  private long timeoutAt;
  private long startTime;
  protected List<String> allColumns;
  private long limit;
  private List<BaseObjectColumnValueSelector<?>> columnSelectors;
  protected long rowCount;
  private Cursor cursor;
  private long targetCount;

  public CursorReader(CursorReaderDefn defn, FragmentContext context)
  {
    this.defn = defn;
    this.responseContext = context.responseContext();
  }

  @Override
  public void start() {
    timeoutAt = (long) responseContext.get(ResponseContext.Key.TIMEOUT_AT);
    startTime = System.currentTimeMillis();
    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, 0L);
    limit = defn.query.getScanRowsLimit();
    if (defn.limitType() == CursorReaderDefn.Limit.GLOBAL) {
      limit -= (Long) responseContext.get(ResponseContext.Key.NUM_SCANNED_ROWS);
    }
    switch (defn.query.getResultFormat()) {
    case RESULT_FORMAT_LIST:
      batchReader = new ListOfMapBatchReader();
      break;
    case RESULT_FORMAT_COMPACTED_LIST:
      batchReader = new CompactListBatchReader();
      break;
    default:
      throw new UOE("resultFormat[%s] is not supported", defn.query.getResultFormat().toString());
    }
    final StorageAdapter adapter = defn.segment.asStorageAdapter();
    if (adapter == null) {
      throw new ISE(
          "Null storage adapter found. Probably trying to issue a query against a segment being memory unmapped."
      );
    }
    if (defn.isWildcard()) {
      allColumns = inferColumns(adapter);
    } else {
      allColumns = defn.columns;
    }
    iter = SequenceIterator.of(adapter.makeCursors(
            defn.filter,
            defn.interval(),
            defn.query.getVirtualColumns(),
            Granularities.ALL,
            defn.isDescendingOrder(),
            null
        ));
  }

  protected List<String> inferColumns(StorageAdapter adapter)
  {
    List<String> cols = new ArrayList<>();
    final Set<String> availableColumns = Sets.newLinkedHashSet(
        Iterables.concat(
            Collections.singleton(defn.isLegacy ? LEGACY_TIMESTAMP_KEY : ColumnHolder.TIME_COLUMN_NAME),
            Iterables.transform(
                Arrays.asList(defn.query.getVirtualColumns().getVirtualColumns()),
                VirtualColumn::getOutputName
            ),
            adapter.getAvailableDimensions(),
            adapter.getAvailableMetrics()
        )
    );

    cols.addAll(availableColumns);

    if (defn.isLegacy) {
      cols.remove(ColumnHolder.TIME_COLUMN_NAME);
    }
    return cols;
  }

  @Override
  public boolean hasNext()
  {
    while (true) {
      // No cursor? Start, done, or end of batch
      if (cursor == null) {
        if (iter == null) {
          return false; // Done previously
        }
        if (!iter.hasNext()) {
          finish();
          return false; // Done now
        }
        cursor = iter.next(); // Next cursor
        createColumnMap();
      } else if (atLimit()) {
        finish();
        return false; // Done now due to limit reached
      } else if (cursor.isDone()) {
        cursor = null; // This cursor done, try the next
      } else {
        return true; // Good to read a batch from this cursor
      }
    }
  }

  private boolean atLimit()
  {
    return rowCount >= limit;
  }

  private void createColumnMap()
  {
    columnSelectors = new ArrayList<>(allColumns.size());
    for (String column : allColumns) {
      final BaseObjectColumnValueSelector<?> selector;
      if (defn.isLegacy && LEGACY_TIMESTAMP_KEY.equals(column)) {
        selector = cursor.getColumnSelectorFactory()
                         .makeColumnValueSelector(ColumnHolder.TIME_COLUMN_NAME);
      } else {
        selector = cursor.getColumnSelectorFactory().makeColumnValueSelector(column);
      }
      columnSelectors.add(selector);
    }
  }

  private void finish()
  {
    iter = null;
    responseContext.add(ResponseContext.Key.NUM_SCANNED_ROWS, rowCount);
    if (defn.hasTimeout()) {
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
    if (defn.hasTimeout() && System.currentTimeMillis() >= timeoutAt) {
      throw new QueryTimeoutException(StringUtils.nonStrictFormat("Query [%s] timed out", defn.query.getId()));
    }
    targetCount = Math.min(limit - rowCount, rowCount + defn.batchSize);
    return new ScanResultValue(
        defn.segmentId,
        allColumns,
        batchReader.read());
  }

  private void advance()
  {
    cursor.advance();
    rowCount++;
  }

  private boolean hasNextRow()
  {
    return !cursor.isDone() && rowCount < targetCount;
  }

  private Object getColumnValue(int i)
  {
    final BaseObjectColumnValueSelector<?> selector = columnSelectors.get(i);
    final Object value;

    // TODO: optimize this to avoid compare on each get
    if (defn.isLegacy && allColumns.get(i).equals(LEGACY_TIMESTAMP_KEY)) {
      value = DateTimes.utc((long) selector.getObject());
    } else {
      value = selector == null ? null : selector.getObject();
    }

    return value;
  }

  @Override
  public void close()
  {
    iter = null;
  }
}
