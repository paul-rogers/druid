package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;

import com.google.common.base.Preconditions;

/**
 * Trivial operator which only reports missing segments. Should be replaced
 * by something simpler later on.
 *
 * @see {@link org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner}
 */
public class MissingSegmentsOperator implements Operator
{
  private static final Logger LOG = new Logger(MissingSegmentsOperator.class);

  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.isEmpty());
      return new MissingSegmentsOperator((Defn) defn, context);
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends LeafDefn
  {
    private final List<SegmentDescriptor> descriptors;

    public Defn(List<SegmentDescriptor> descriptors)
    {
      this.descriptors = descriptors;
    }
  }

  private final Defn defn;
  private final FragmentContext context;

  public MissingSegmentsOperator(Defn defn, FragmentContext context)
  {
    this.defn = defn;
    this.context = context;
  }

  @Override
  public Iterator<Object> open() {
    LOG.debug("Reporting a missing segments[%s] for query[%s]", defn.descriptors, context.queryId());
    context.responseContext().add(ResponseContext.Key.MISSING_SEGMENTS, defn.descriptors);
    return Collections.emptyIterator();
  }

  @Override
  public void close(boolean cascade)
  {
  }
}
