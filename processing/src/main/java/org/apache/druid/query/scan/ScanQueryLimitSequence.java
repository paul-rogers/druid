package org.apache.druid.query.scan;

import java.util.ArrayList;
import java.util.List;

import org.apache.druid.java.util.common.UOE;
import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.java.util.common.guava.Yielder;
import org.apache.druid.java.util.common.guava.YieldingAccumulator;
import org.apache.druid.query.Query;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.utils.CloseableUtils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ScanQueryLimitSequence extends TransformSequence<ScanResultValue>
{
  public static ScanQueryLimitSequence forQuery(
      final QueryRunner<ScanResultValue> runner,
      final QueryPlus<ScanResultValue> queryPlus,
      final ResponseContext responseContext
  )
  {
    ScanQuery query = (ScanQuery) queryPlus.getQuery();
    ScanQuery.ResultFormat resultFormat = query.getResultFormat();
    if (ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR.equals(resultFormat)) {
      throw new UOE(ScanQuery.ResultFormat.RESULT_FORMAT_VALUE_VECTOR + " is not supported yet");
    }
    boolean grouped = query.getOrder() == ScanQuery.Order.NONE ||
        !query.getContextBoolean(ScanQuery.CTX_KEY_OUTERMOST, true);
    return new ScanQueryLimitSequence(
        query.getScanRowsLimit(),
        grouped,
        query.getBatchSize(),
        new InputCreator<ScanResultValue>() {
          @Override
          public Sequence<ScanResultValue> open()
          {
            Query<ScanResultValue> historicalQuery =
                queryPlus.getQuery().withOverriddenContext(ImmutableMap.of(ScanQuery.CTX_KEY_OUTERMOST, false));
            return runner.run(QueryPlus.wrap(historicalQuery), responseContext);
          }
        });
    }

  InputCreator<ScanResultValue> child;
  private final boolean grouped;
  private final int batchSize;
  private final long limit;
  private Yielder<ScanResultValue> inputYielder;
  private long rowCount;

  public ScanQueryLimitSequence(long limit, boolean grouped, int batchSize, InputCreator<ScanResultValue> child)
  {
    this.child = child;
    this.limit = limit;
    this.grouped = grouped;
    this.batchSize = batchSize;
  }

  @Override
  protected void open()
  {
    Sequence<ScanResultValue> input = child.open();
    inputYielder = input.toYielder(
        null,
        new YieldingAccumulator<ScanResultValue, ScanResultValue>()
        {
          @Override
          public ScanResultValue accumulate(ScanResultValue accumulated, ScanResultValue in)
          {
            yield();
            return in;
          }
        });
  }

  @Override
  protected boolean hasNext() {
    return rowCount < limit && hasNextInput();
  }

  @Override
  protected ScanResultValue next() {
    if (grouped) {
      return groupedNext();
    } else {
      return ungroupedNext();
    }
  }

  private boolean hasNextInput()
  {
    return inputYielder != null && !inputYielder.isDone();
  }

  private ScanResultValue nextInput()
  {
    ScanResultValue value = inputYielder.get();
    inputYielder = inputYielder.next(null);
    return value;
  }

  private ScanResultValue groupedNext() {
    ScanResultValue batch = nextInput();
    List<?> events = (List<?>) batch.getEvents();
    if (events.size() <= limit - rowCount) {
      rowCount += events.size();
      return batch;
    } else {
      // last batch
      // single batch length is <= rowCount.MAX_VALUE, so this should not overflow
      int numLeft = (int) (limit - rowCount);
      rowCount = limit;
      return new ScanResultValue(batch.getSegmentId(), batch.getColumns(), events.subList(0, numLeft));
    }
  }

  private ScanResultValue ungroupedNext() {
    // Perform single-event ScanResultValue batching at the outer level.  Each scan result value from the yielder
    // in this case will only have one event so there's no need to iterate through events.
    List<Object> eventsToAdd = new ArrayList<>(batchSize);
    List<String> columns = new ArrayList<>();
    while (eventsToAdd.size() < batchSize && hasNextInput() && rowCount < limit) {
      ScanResultValue srv = nextInput();
      // Only replace once using the columns from the first event
      columns = columns.isEmpty() ? srv.getColumns() : columns;
      eventsToAdd.add(Iterables.getOnlyElement((List<?>) srv.getEvents()));
      rowCount++;
    }
    return new ScanResultValue(null, columns, eventsToAdd);
  }

  @Override
  protected void close()
  {
    try {
      CloseableUtils.closeAndWrapExceptions(inputYielder);
    }
    finally {
      inputYielder = null;
    }
  }
}
