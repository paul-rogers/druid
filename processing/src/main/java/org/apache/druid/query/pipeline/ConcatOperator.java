package org.apache.druid.query.pipeline;

import java.util.Iterator;
import java.util.List;

import org.apache.druid.query.pipeline.Operator.IterableOperator;

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
  public static Operator concatOrNot(List<Operator> children) {
    if (children.size() > 1) {
      return new ConcatOperator(children);
    }
    return children.get(0);
  }

  private final Iterator<Operator> childIter;
  private FragmentContext context;
  private Operator current;
  private Iterator<Object> currentIter;

  public ConcatOperator(List<Operator> children) {
    childIter = children.iterator();
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    this.context = context;
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
      currentIter = current.open(context);
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
