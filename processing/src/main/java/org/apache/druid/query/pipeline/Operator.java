package org.apache.druid.query.pipeline;

import java.io.Closeable;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;

/**
 * An operator is a data pipeline transform: something that changes a stream of
 * results in some way. An operator is an extended iterator and has a very simple
 * lifecycle:
 * <p>
 * <ul>
 * <li>Created (from a definition via a factory).</li>
 * <li>Opened (once, bottom up in the operator DAG).</li>
 * <li>A set of hasNext(), next() pairs until hasNext() returns false.</li>
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
 * Operators <i>do not</i> cascade <code>start()</code> and <code>close()</code>
 * operations to their children: that is the job of the {@link FragmentRunner}
 * which manages the operators. This way, operators don't have to contain
 * redundant code to manage errors and the like during these operations.
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
 */
public interface Operator extends Iterator<Object>
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
    Operator build(OperatorDefn defn, List<Operator> children, FragmentContext context);
  }

  void start();
  void close();
}
