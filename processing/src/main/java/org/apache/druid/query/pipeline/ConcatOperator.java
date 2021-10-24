package org.apache.druid.query.pipeline;

import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;

import com.google.common.base.Preconditions;

/**
 * Concatenate a series of inputs. Simply returns the input values,
 * does not limit or coalesce batches. Starts child operators as late as
 * possible, and closes them as early as possible.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryRunnerFactory#mergeRunners}
 */
public class ConcatOperator implements Operator
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

  public static class Defn extends MultiChildDefn
  {
    public Defn(List<OperatorDefn> children)
    {
      this.children = children;
    }
  }

  private final Iterator<Operator> childIter;
  private Operator current;

  public ConcatOperator(Defn defn, List<Operator> children,
      FragmentContext context) {
    childIter = children.iterator();
  }

  @Override
  public void start()
  {
  }

  @Override
  public boolean hasNext() {
    while (true) {
      if (current != null) {
        if (current.hasNext()) {
          return true;
        }
        current.close(true);
        current = null;
      }
      if (!childIter.hasNext()) {
        return false;
      }
      current = childIter.next();
      current.start();
    }
  }

  @Override
  public Object next() {
    Preconditions.checkState(current != null, "Missing call to hasNext()?");
    return current.next();
  }

  @Override
  public void close(boolean cascade) {
    // If cascade = false, fragment runner will close child
    if (cascade && current != null) {
      current.close(cascade);
      current = null;
    }
  }
}
