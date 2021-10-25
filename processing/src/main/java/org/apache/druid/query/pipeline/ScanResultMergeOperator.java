package org.apache.druid.query.pipeline;

import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.Ordering;

/**
 * Performs an n-way merge on n ordered child operators.
 * <p>
 * Returns elements from the priority queue in order of increasing priority, here
 * defined as in the desired output order.
 * This is due to the fact that PriorityQueue#remove() polls from the head of the queue which is, according to
 * the PriorityQueue javadoc, "the least element with respect to the specified ordering"
 *
 * @see {@link org.apache.druid.java.util.common.guava.MergeSequence}
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#nWayMergeAndLimit}
*/
public class ScanResultMergeOperator implements Operator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(!children.isEmpty());
      return new ScanResultMergeOperator((Defn) defn, children, context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends MultiChildDefn
  {
    public Ordering<ScanResultValue> ordering;

    public Defn(ScanQuery query, List<OperatorDefn> children)
    {
      this.children = children;
      this.ordering = query.getResultOrdering();
    }
  }

  private static class Entry
  {
    final Operator child;
    ScanResultValue row;

    public Entry(Operator child, ScanResultValue row) {
      this.child = child;
      this.row = row;
    }
  }

  private final List<Operator> children;
  private final PriorityQueue<Entry> pQueue;

  public ScanResultMergeOperator(Defn defn, List<Operator> children,
      FragmentContext context) {
    this.children = children;
    this.pQueue = new PriorityQueue<>(
        32,
        defn.ordering.onResultOf(
            (Function<Entry, ScanResultValue>) input -> input.row
        )
    );
  }

  @Override
  public void start() {
    for (int i = 0; i < children.size(); i++) {
      Operator input = children.get(i);
      input.start();
      if (input.hasNext()) {
        pQueue.add(new Entry(input, (ScanResultValue) input.next()));
      }
    }
  }

  @Override
  public boolean hasNext() {
    return !pQueue.isEmpty();
  }

  @Override
  public Object next() {
    Entry entry = pQueue.remove();
    Object row = entry.row;
    if (entry.child.hasNext()) {
      entry.row = (ScanResultValue) entry.child.next();
      pQueue.add(entry);
    } else {
      entry.child.close(true);
    }
    return row;
  }

  @Override
  public void close(boolean cascade)
  {
    if (!cascade) {
      return;
    }
    while (!pQueue.isEmpty()) {
      pQueue.remove().child.close(cascade);
    }
  }
}
