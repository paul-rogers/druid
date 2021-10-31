package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.query.SegmentDescriptor;
import org.apache.druid.query.context.ResponseContext;

/**
 * Trivial operator which only reports missing segments. Should be replaced
 * by something simpler later on.
 *
 * @see {@link org.apache.druid.query.ReportTimelineMissingSegmentQueryRunner}
 */
public class MissingSegmentsOperator implements Operator
{
  private static final Logger LOG = new Logger(MissingSegmentsOperator.class);

  private final List<SegmentDescriptor> descriptors;

  public MissingSegmentsOperator(List<SegmentDescriptor> descriptors)
  {
    this.descriptors = descriptors;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    LOG.debug("Reporting a missing segments[%s] for query[%s]", descriptors, context.queryId());
    context.responseContext().add(ResponseContext.Key.MISSING_SEGMENTS, descriptors);
    return Collections.emptyIterator();
  }

  @Override
  public void close(boolean cascade)
  {
  }
}
