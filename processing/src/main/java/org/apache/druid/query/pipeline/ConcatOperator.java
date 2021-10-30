package org.apache.druid.query.pipeline;

import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.pipeline.Operator.IterableOperator;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;

import com.google.common.base.Preconditions;

/**
 * Concatenate a series of inputs. Simply returns the input values,
 * does not limit or coalesce batches. Starts child operators as late as
 * possible, and closes them as early as possible.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
 */
public class ConcatOperator implements IterableOperator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(!children.isEmpty());
      return new ConcatOperator((Defn) defn, children, context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static OperatorDefn concatOrNot(List<OperatorDefn> children) {
    if (children.size() > 1) {
      return new ConcatOperator.Defn(children);
    }
    return children.get(0);
  }

  public static class Defn extends MultiChildDefn
  {
    public Defn(List<OperatorDefn> children)
    {
      this.children = children;
    }
  }

  private final Iterator<Operator> childIter;
  private Operator current;
  private Iterator<Object> currentIter;

  public ConcatOperator(Defn defn, List<Operator> children,
      FragmentContext context) {
    childIter = children.iterator();
  }

  @Override
  public Iterator<Object> open()
  {
    return this;
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (current != null) {
        if (currentIter.hasNext()) {
          return true;
        }
        current.close(true);
        current = null;
        currentIter = null;
      }
      if (!childIter.hasNext()) {
        return false;
      }
      current = childIter.next();
      currentIter = current.open();
    }
  }

  @Override
  public Object next() {
    Preconditions.checkState(currentIter != null, "Missing call to hasNext()?");
    return currentIter.next();
  }

  @Override
  public void close(boolean cascade) {
    if (cascade && current != null) {
      current.close(cascade);
    }
    current = null;
    currentIter = null;
  }
}
