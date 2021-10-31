package org.apache.druid.query.pipeline;

import java.util.Iterator;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.profile.Timer;

import com.google.common.base.Preconditions;

/**
 * Operator to emit runtime metrics. This is a temporary solution: these
 * metrics are better emitted at the top of the stack by the fragment
 * runner to avoid the per-row overhead.
 *
 * @see {@link org.apache.druid.query.MetricsEmittingQueryRunner}
 */
public class MetricsOperator implements Operator
{
  private static final Logger log = new Logger(MetricsOperator.class);

  private final Timer waitTimer = Timer.createStarted();
  private final ServiceEmitter emitter;
  private final String segmentIdString;
  private final QueryMetrics<?> queryMetrics;
  private final Operator child;
  private final Timer runTimer = Timer.create();
  private State state = State.START;

  public MetricsOperator(
          final ServiceEmitter emitter,
          final String segmentIdString,
          final QueryMetrics<?> queryMetrics,
          final Operator child
  )
  {
    this.emitter = emitter;
    this.segmentIdString = segmentIdString;
    this.queryMetrics = queryMetrics;
    this.child = child;
  }

  @Override
  public Iterator<Object> open(FragmentContext context) {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    runTimer.start();
    queryMetrics.segment(segmentIdString);
    return child.open(context);
  }

  @Override
  public void close(boolean cascade) {
    if (state != State.RUN) {
      state = State.CLOSED;
      return;
    }
    state = State.CLOSED;
    if (cascade) {
      child.close(cascade);
    }
    queryMetrics.reportSegmentTime(runTimer.get());
    queryMetrics.reportWaitTime(waitTimer.get() - runTimer.get());
    try {
      queryMetrics.emit(emitter);
    }
    catch (Exception e) {
      // Query should not fail, because of emitter failure. Swallowing the exception.
      log.error("Failure while trying to emit [%s] with stacktrace [%s]", emitter.toString(), e);
    }
  }
}
