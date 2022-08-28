package org.apache.druid.queryng.operator;

import org.apache.druid.query.Druids;
import org.apache.druid.query.QueryPlus;
import org.apache.druid.query.QueryRunner;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.NullOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorTest;
import org.apache.druid.queryng.operators.Operators;
import org.apache.druid.queryng.operators.scan.MockScanResultReader;
import org.apache.druid.segment.SegmentMissingException;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

@Category(OperatorTest.class)
public class RetryOperatorTest
{
  /**
   * Input has no rows, no missing segments.
   */
  @Test
  public void testEmptyInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    AtomicBoolean didRun = new AtomicBoolean();
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        context,
        queryPlus,
        new NullOperator<ScanResultValue>(context),
        null,
        (id, ctx) -> Collections.emptyList(),
        1,
        true,
        () -> { didRun.set(true); }
        );
    assertTrue(Operators.toList(op).isEmpty());
    assertTrue(didRun.get());
  }

  /**
   * Input has values and no missing segments.
   */
  @Test
  public void testSimpleInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(context, 3, 10, 4, MockScanResultReader.interval(0));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        context,
        queryPlus,
        scan,
        null,
        (id, ctx) -> Collections.emptyList(),
        1,
        true,
        () -> { }
        );
    List<ScanResultValue> results = Operators.toList(op);
    assertEquals(3, results.size());
  }

  /**
   * One round of missing segments, second round is fine.
   */
  @Test
  public void testRetryInput()
  {
    FragmentContext context = FragmentContext.defaultContext();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(context, 3, 10, 4, MockScanResultReader.interval(0));
    Operator<ScanResultValue> scan2 = new MockScanResultReader(context, 3, 5, 4, MockScanResultReader.interval(0));
    QueryRunner<ScanResultValue> runner2 = (qp, qc) -> Operators.toSequence(scan2);
    AtomicInteger counter = new AtomicInteger();
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        context,
        queryPlus,
        scan,
        (q, segs) -> runner2,
        (id, ctx) -> {
          if (counter.getAndAdd(1) == 0) {
            return dummySegs;
          } else {
            return Collections.emptyList();
          }
        },
        2,
        true,
        () -> { }
        );
    List<ScanResultValue> results = Operators.toList(op);
    assertEquals(5, results.size());
  }

  /**
   * Continuous missing segments, hit limit of 2, but query allows partial results.
   */
  @Test
  public void testRetryLimitPartial()
  {
    FragmentContext context = FragmentContext.defaultContext();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(context, 3, 10, 4, MockScanResultReader.interval(0));
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        context,
        queryPlus,
        scan,
        (q, segs) -> {
          Operator<ScanResultValue> scan2 = new MockScanResultReader(context, 3, 5, 4, MockScanResultReader.interval(0));
          return (qp, qc) -> Operators.toSequence(scan2);
        },
        (id, ctx) -> dummySegs,
        2,
        true,
        () -> { }
        );
    List<ScanResultValue> results = Operators.toList(op);
    assertEquals(7, results.size());
  }

  /**
   * Continuous missing segments, hit limit of 2, no partial results,
   * so fails.
   */
  @Test
  public void testRetryLimit()
  {
    FragmentContext context = FragmentContext.defaultContext();
    ScanQuery query = Druids.newScanQueryBuilder()
        .dataSource("foo")
        .eternityInterval()
        .build();
    QueryPlus<ScanResultValue> queryPlus = QueryPlus.wrap(query.withId("dummy"));
    Operator<ScanResultValue> scan = new MockScanResultReader(context, 3, 10, 4, MockScanResultReader.interval(0));
    List<SegmentDescriptor> dummySegs = Arrays.asList(
        new SegmentDescriptor(MockScanResultReader.interval(0), "vers", 1));
    Operator<ScanResultValue> op = new RetryOperator<ScanResultValue>(
        context,
        queryPlus,
        scan,
        (q, segs) -> {
          Operator<ScanResultValue> scan2 = new MockScanResultReader(context, 3, 5, 4, MockScanResultReader.interval(0));
          return (qp, qc) -> Operators.toSequence(scan2);
        },
        (id, ctx) -> dummySegs,
        2,
        false,
        () -> { }
        );
    try {
      Operators.toList(op);
      fail();
    }
    catch (SegmentMissingException e) {
      // Expected
    }
  }
}
