package org.apache.druid.queryng.fragment;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.queryng.operators.Operator;

import java.util.Iterator;
import java.util.List;

/**
 * Build a fragment dynamically. For use during the transition when query runners
 * build operators. Once built, the fragment can be run as a root operator, a
 * sequence, or materialized into a list. The fragment can be run only once.
 */
public interface FragmentBuilder
{
  /**
   * Specialized, temporary root iterator which provides access to
   * results, while running a fragment internally. The caller is required to
   * call {@link close}.
   */
  interface ResultIterator<T> extends Iterator<T>, AutoCloseable
  {
    @Override
    void close();
  }

  /**
   * Add an operator dynamically. The operator will be closed
   * at query completion.
   */
  void register(Operator<?> op);

  /**
   * Return a root operator for the fragment assuming the most recently
   * registered operator is the root.
   */
  <T> ResultIterator<T> open();

  /**
   * Return the root operator with the given operator as the root of the
   * operator DAG.
   */
  <T> ResultIterator<T> open(Operator<T> op);

  /**
   * Provide the fragment results as a sequence. The sequence closes the
   * fragment at sequence closure.
   */
  <T> Sequence<T> toSequence();
  <T> Sequence<T> toSequence(Operator<T> op);

  /**
   * Materializes the entire result set as a list. Primarily for testing.
   * Opens the fragment, reads results, and closes the fragment.
   */
  <T> List<T> toList();
  <T> List<T> toList(Operator<T> rootOp);

  FragmentContext context();

  /**
   * A simple fragment builder for testing.
   */
  static FragmentBuilder defaultBuilder()
  {
    return new FragmentBuilderImpl(
        "unknown",
        FragmentContext.NO_TIMEOUT,
        ResponseContext.createEmpty());
  }

}
