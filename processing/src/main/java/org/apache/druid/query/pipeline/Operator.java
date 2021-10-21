package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.List;

/**
 * An operator is a data pipeline transform: something that changes a stream of
 * results in some way. An operator has a very simple lifecycle:
 * <p>
 * <ul>
 * <li>Created (from a definition via a factory).</li>
 * <li>Opened (once, bottom up in the operator DAG).</li>
 * <li>A set of next(), get() pairs until next() returns false.</li>
 * <li>Closed (once, bottom up in the operator DAG).</li>
 * </ul>
 * <p>
 * Leaf operators produce results, internal operators transform them, and root
 * operators do something with the results. Operators no nothing about their
 * children other than that they follow the operator protocol and will produce a
 * result when asked. Operators must agree on the type of the shared results.
 * <p>
 * Unlike traditional <code>QueryRunner</code>s, a operator does not create its
 * children: that is the job of the planner that created a tree of operator
 * definitions. The operator simply accepts the previously-created children and
 * does its thing.
 * <p>
 * Operators <i>do not</i> cascade <code>start()</code> and <code>close()</code>
 * operations to their children: that is the job of the {@link FragmentRunner}
 * which manages the operators. This way, operators don't have to contain
 * redundant code to manage errors and the like during these operations.
 * <p>
 * Operators are assumed to be stateful since most data transform operations
 * involve some kind of state. Resources should be allocated in
 * <code>start()</code> (or later), and released in <code>close()</code>.
 * Implementations can assume that calls to <code>next()</code> and
 * <code>get()</code> occur within a single thread, so state can be maintained
 * in normal (not atomic) variables.
 * <p>
 * Once an operator returns <code>false</code> from <code>next()</code>, then
 * the caller should not call <code>get()</code>. It should be benign to call
 * <code>next()</code> after the first <code>false</code> return: all subsequent
 * calls should also return <code>false</code>.
 * <p>
 * There must be one call to <code>get()</code> for each call to
 * <code>next()</code> which returned <code>true</code>. That is, it is the
 * caller's responsibility to accept and manage resources for the output value.
 * That said, each operator should be prepared to release any un-gotten result
 * when <code>close()</code> is called: this may signal that an error occurred
 * during processing.
 * <p>
 * Operators are one-pass: they are not re-entrant and cannot be restarted. We
 * assume that they read ephemeral values: once returned, they are gone. We also
 * assume the results are volatile: once read, we may not be able to read the
 * same set of values a second time even if we started the whole process over.
 *
 * @param <T>
 *          The type of the results this operator produces. The type is
 *          typically some batch of rows rather than individual rows, but the
 *          operator protocol is agnostic.
 */
public interface Operator
{
  /**
   * Operator definition: provides everything that the operator needs to do
   * its job, or that the factory needs to choose among a set of closely-related
   * operators. Thus, there is usually a one-to-one relationship between
   * definitions and operators, but there may be a one-to-many relationship.
   */
  interface OperatorDefn
  {
    List<OperatorDefn> children();
  }

  /**
   * Abstract base class that assumes the operator is a leaf.
   */
  public abstract class AbstractOperatorDefn implements OperatorDefn
  {
    @Override
    public List<OperatorDefn> children() {
      return Collections.emptyList();
    }
  }

  /**
   * Class which takes an operator definition and produces an operator for
   * that definition.
   */
  interface OperatorFactory
  {
    Operator build(OperatorDefn defn, List<Operator> children);
  }

  void start();
  boolean next();
  Object get();
  void close();
}
