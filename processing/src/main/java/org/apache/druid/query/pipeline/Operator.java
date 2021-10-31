package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.context.ResponseContext;

/**
 * An operator is a data pipeline transform: something that changes a stream of
 * results in some way. An operator has a very simple lifecycle:
 * <p>
 * <ul>
 * <li>Created (from a definition via a factory).</li>
 * <li>Opened (once, bottom up in the operator DAG), which provides an
 * iterator over the results fo this operator.</li>
 * <li>Closed.</li>
 * </ul>
 * <p>
 * Leaf operators produce results, internal operators transform them, and root
 * operators do something with the results. Operators know nothing about their
 * children other than that they follow the operator protocol and will produce a
 * result when asked. Operators must agree on the type of the shared results.
 * <p>
 * Unlike traditional <code>QueryRunner</code>s, an operator does not create its
 * children: that is the job of the planner that created a tree of operator
 * definitions. The operator simply accepts the previously-created children and
 * does its thing.
 * <p>
 * Operators are assumed to be stateful since most data transform operations
 * involve some kind of state. The {@code start()} and {@code close()}
 * methods provide a well-defined way
 * to handle resource. Operators are created as a DAG. Unlike a simple iterator,
 * operators should <i>not</i> obtain resources in their constructors. Instead,
 * they should obtain resources, open files or otherwise start things whirring in
 * the {#code start()} method. In some cases, resource may be obtained a bit later,
 * in the first call to {@code hasNext()}. In either case, resources should be
 * released in {@code close()}.
 * <p>
 * Operators may appear in a branch of the query DAG that can be deferred or
 * closed early. Typical example: a UNION operator that starts its first child,
 * runs it, closes it, then moves to the next child. This form of operation ensures
 * resources are held for the briefest possible period of time.
 * <p>
 * To make this work, operators should cascade <code>start()</code> and
 * <code>close()</code> operations to their children. The query runner will
 * start the root operator: the root must start is children, and so on. Closing is
 * a bit more complex. Operators should close their children by cascading the
 * close operation: but only if that call came from a parent operator (as
 * indicated by the {@code cascade} parameter set to {@code true}.)
 * <p>
 * The {@link FragmentRunner} will ensure all operators are closed by calling:
 * close, from the bottom up, at the completion (successful or otherwise) of the
 * query. In this case, the  {@code cascade} parameter set to {@code false},
 * and each operator <i>should not</i> cascade this call to its children: the
 * fragment runner will do so.
 * <p>
 * Implementations can assume that calls to <code>next()</code> and
 * <code>get()</code> occur within a single thread, so state can be maintained
 * in normal (not atomic) variables.
 * <p>
 * Operators are one-pass: they are not re-entrant and cannot be restarted. We
 * assume that they read ephemeral values: once returned, they are gone. We also
 * assume the results are volatile: once read, we may not be able to read the
 * same set of values a second time even if we started the whole process over.
 * <p>
 * The type of the values returned by the operator is determined by context.
 * Druid native queries use a variety of Java objects: there is no single
 * "row" class or interface.
 * <p>
 * Operators do not have a return type parameter. Operators are generally created
 * dynamically: that code is far simpler without having to deal with unknown
 * types. Even test code will often call {@code assertEquals()} and the like
 * which don't need the type.
 * <p>
 * Having {@code open()} return the iterator for results accomplishes two goals.
 * First is the minor benefit of ensuring that an operator is opened before
 * fetching results. More substantially, this approach allows "wrapper" operators
 * which only perform work in the open or close method. For those, the open
 * method returns the iterator of the child, avoiding the overhead of pass-through
 * calls for each data batch. The wrapper operator will not sit on the data
 * path, only on the control (open/close) path.
 */
public interface Operator
{

//  /**
//   * Add-on functionality for an operator. We assume the add-on only needs to
//   * run at the start and end: if it wanted to do something on every row, it
//   * would be an operator. Allows capturing information about an operation
//   * without the per-batch overhead of doing so in an actual operator.
//   *
//   * @see {@link org.apache.druid.java.util.common.guava.SequenceWrapper}
//   */
//  public interface Decorator
//  {
//    void before();
//    void after();
//  }

  /**
   * Operator definition: provides everything that the operator needs to do
   * its job, or that the factory needs to choose among a set of closely-related
   * operators. Thus, there is usually a one-to-one relationship between
   * definitions and operators, but there may be a one-to-many relationship.
   */
  interface OperatorDefn
  {
    List<OperatorDefn> children();
    Operator decorate(Operator op);
  }

  public abstract class BaseDefn implements OperatorDefn
  {
    @Override
    public Operator decorate(Operator op)
    {
      return op;
    }
  }

  /**
   * Abstract base class that assumes the operator is a leaf.
   */
  public abstract class LeafDefn extends BaseDefn
  {
    @Override
    public List<OperatorDefn> children() {
      return Collections.emptyList();
    }
  }

  /**
   * Abstract base class for operators with one child.
   */
  public abstract class SingleChildDefn extends BaseDefn
  {
    public OperatorDefn child;

    @Override
    public List<OperatorDefn> children() {
      return Collections.singletonList(child);
    }
  }

  /**
   * Abstract base class for operators with multiple children.
   */
  public abstract class MultiChildDefn extends BaseDefn
  {
    public List<OperatorDefn> children;

    @Override
    public List<OperatorDefn> children() {
      return children;
    }
  }

  /**
   * Class which takes an operator definition and produces an operator for
   * that definition.
   */
  public interface OperatorFactory
  {
    Operator build(OperatorDefn defn, List<Operator> children, FragmentContext context);
  }

  public interface FragmentContext
  {
    String queryId();
    ResponseContext responseContext();
  }

  public static class FragmentContextImpl implements FragmentContext
  {
    private final ResponseContext responseContext;
    private final String queryId;

    public FragmentContextImpl(String queryId, ResponseContext responseContext)
    {
      this.queryId = queryId;
      this.responseContext = responseContext;
    }

    @Override
    public String queryId() {
      return queryId;
    }

    @Override
    public ResponseContext responseContext() {
      return responseContext;
    }
  }

  public static FragmentContext defaultContext() {
    return new FragmentContextImpl("unknown", ResponseContext.createEmpty());
  }

  /**
   * Convenience interface for an operator which is its own iterator.
   */
  public interface IterableOperator extends Operator, Iterator<Object>
  {
  }

  /**
   * State used to track the lifecycle of an operator when we
   * need to know.
   */
  public enum State
  {
    START, RUN, CLOSED
  }

  /**
   * Called to prepare for processing. Allows the operator to be created early in
   * the run, but resources to be obtained as late as possible in the run. An operator
   * calls{@code open()} on its children when it is ready to read its input: either
   * in the {@code open()} call for simple operators,or later, on demand, for more
   * complex operators such as in a merge or union.
   */
  Iterator<Object> open(FragmentContext context);

  /**
   * Called at two distinct times. An operator may choose to close a child
   * when it is clear that the child will no longer be needed. For example,
   * a union might close its first child when it moves onto the second.
   * <p>
   * Operators should handle at least two calls to @{code close()}: an optional
   * early close, and a definite final close when the fragment runner shuts
   * down.
   * <p>
   * Because the fragment runner will ensure a final close, operators are
   * not required to ensure {@code close()} is called on children for odd
   * paths, such as errors.
   *
   * @param cascade {@code false} if this is the final call from the fragment
   * runner, {@code true} if it is an "early close" from a parent.
   */
  void close(boolean cascade);
}
