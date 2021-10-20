package org.apache.druid.query.pipeline;

import com.google.common.base.Preconditions;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.pipeline.Operator.OperatorFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

/**
 * Mechanism to manage a DAG (typically a tree in Druid) of operators. The operators form
 * a data pipeline: data flows from leaves though internal nodes to the root. When the DAG
 * defines the entire pipeline on one node, it forms a fragment of a larger query which
 * typically runs on multiple nodes.
 * <p>
 * The fragment is defined via a parallel tree of operator definitions emitted by a planner.
 * The definitions are static, typically reflect aspects of the user's request and system
 * metadata, but know nothing about the actual rows. The definitions give rise (via
 * operator factories) to the actual stateful operator DAG which runs the pipeline.
 * <p>
 * The fragment runner provides a uniform bottom-to-top protocol to start and stop the
 * pipeline, allowing operators to worry only about their own needs, but not the needs
 * of their parents or children.
 * <p>
 * When building the operator DAG, the runner creates the leaves first, then passes
 * these as children to the next layer, and so on up to the root.
 */
public class FragmentRunner
{
  /**
   * Registry, typically global, but often ad-hoc for tests, which maps from operator
   * definition classes to the corresponding operator factory.
   */
  public static class OperatorRegistry
  {
    private Map<Class<? extends OperatorDefn>, OperatorFactory<?>> factories = new IdentityHashMap<>();

    public void register(Class<? extends OperatorDefn> defClass, OperatorFactory<?> factory)
    {
      Preconditions.checkState(!factories.containsKey(defClass));
      factories.put(defClass, factory);
    }

    public OperatorFactory<?> factory(OperatorDefn defn)
    {
      return Preconditions.checkNotNull(factories.get(defn.getClass()));
    }
  }

  /**
   * The consumer is a class which accepts each row produced by the runner
   * and does something with it. The consumer returns <code>true</code> if
   * it wants more rows, </code>false</code> if it wants to terminate
   * results early.
   *
   * @param <T>
   */
  public interface Consumer
  {
    boolean accept(Object row);
  }

  private final OperatorRegistry registry;
  private final List<Operator<?>> operators = new ArrayList<>();

  public FragmentRunner(OperatorRegistry registry)
  {
    this.registry = registry;
  }

  public Operator<?> build(OperatorDefn rootDefn)
  {
    List<Operator<?>> children = new ArrayList<>();
    for (OperatorDefn child : rootDefn.children()) {
      children.add(build(child));
    }
    OperatorFactory<?> factory = registry.factory(rootDefn);
    Operator<?> rootOp = factory.build(rootDefn, children);
    operators.add(rootOp);
    return rootOp;
  }

  public void start()
  {
    List<Operator<?>> opened = new ArrayList<>();
    for (Operator<?> op : operators) {
      try {
        op.start();
        opened.add(op);
      }
      catch (Exception e) {
        close(opened);
        throw e;
      }
    }
  }

  public void run(Consumer consumer) {
    @SuppressWarnings("unchecked")
    Operator<Object> root = (Operator<Object>) operators.get(operators.size() - 1);
    while (root.next()) {
      if (!consumer.accept(root.get())) {
        break;
      }
    }
  }

  public void close()
  {
    close(operators);
  }

  public void fullRun(Consumer consumer)
  {
    start();
    run(consumer);
    close();
  }

  private void close(List<Operator<?>> ops)
  {
    List<Exception> exceptions = new ArrayList<>();
    for (Operator<?> op : ops) {
      try {
        op.close();
      }
      catch (Exception e) {
        exceptions.add(e);
      }
    }
  }
}
