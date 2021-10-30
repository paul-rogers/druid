package org.apache.druid.query.pipeline;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Optional;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.segment.SegmentReference;

import com.google.common.base.Preconditions;

/**
 * @{see {@link org.apache.druid.query.ReferenceCountingSegmentQueryRunner}
 */
public class SegmentLockOperator implements Operator
{
  private static final Logger LOG = new Logger(SegmentLockOperator.class);

  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new SegmentLockOperator((Defn) defn, context, children.get(0));
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends SingleChildDefn
  {
    private final SegmentReference segment;
    private final SegmentDescriptor descriptor;

    public Defn(
        SegmentReference segment,
        SegmentDescriptor descriptor,
        OperatorDefn child
    )
    {
      this.segment = segment;
      this.descriptor = descriptor;
      this.child = child;
    }
  }

  private final Defn defn;
  private final FragmentContext context;
  private final Operator child;
  private Closeable lock;

  public SegmentLockOperator(Defn defn, FragmentContext context, Operator child)
  {
    this.defn = defn;
    this.context = context;
    this.child = child;
  }

  @Override
  public void start() {
    Optional<Closeable> maybeLock = defn.segment.acquireReferences();
    if (maybeLock.isPresent()) {
      lock = maybeLock.get();
      child.start();
    } else {
      LOG.debug("Reporting a missing segment[%s] for query[%s]", defn.descriptor, context.queryId());
      context.responseContext().add(ResponseContext.Key.MISSING_SEGMENTS, defn.descriptor);
    }
  }

  @Override
  public boolean hasNext() {
    return lock != null && child.hasNext();
  }

  @Override
  public Object next() {
    return child.next();
  }

  @Override
  public void close(boolean cascade) {
    if (lock != null) {
      try {
        lock.close();
      } catch (IOException e) {
        throw new RuntimeException("Failed to close segment " + defn.descriptor.toString(), e);
      }
      lock = null;
    }
    if (cascade) {
      child.close(cascade);
    }
  }
}
