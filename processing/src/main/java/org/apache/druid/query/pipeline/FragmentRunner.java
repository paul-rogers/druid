package org.apache.druid.query.pipeline;

import com.google.common.base.Preconditions;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
import org.apache.druid.query.pipeline.Operator.OperatorFactory;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;

public class FragmentRunner
{
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

  public void close()
  {
    close(operators);
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
