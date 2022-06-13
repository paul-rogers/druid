package org.apache.druid.queryng.operators.general;

import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OrderedMergeOperator.Input;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

/**
 * Scatter portion of the scatter-gather operation, for the case in which
 * segments are local, which generally only occurs in tests.
 * <p>
 * Wraps the input operator in a runner that rewrites the QuerySegmentSpec to mention
 * the specific segments. This mimics what CachingClusteredClient on the Broker
 * does, and is required for certain queries (like Scan) to function properly.
 * SegmentServerSelector does not currently mimic CachingClusteredClient, it uses
 * the LocalQuerySegmentWalker constructor instead since this walker does not
 * mimic remote DruidServer objects to actually serve the queries.
 * <p>
 * This version does not handle scheduling like {@code TestClusterQuerySegmentWalker}
 * does. Scheduling is far better handled higher in the DAG.
 *
 * @see {@link org.apache.druid.server.TestClusterQuerySegmentWalker}
 */
public class LocalFragmentScatter<T> implements Supplier<Iterable<Input<T>>>
{
  private interface ScatterOperatorFactory<T>
  {
    Operator<T> create();
  }

  private final List<Input<T>> inputs = new ArrayList<>();

  @Override
  public Iterable<Input<T>> get()
  {
    return inputs;
  }

}
