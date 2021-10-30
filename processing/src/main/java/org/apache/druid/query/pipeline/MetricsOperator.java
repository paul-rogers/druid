package org.apache.druid.query.pipeline;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.ObjLongConsumer;

import org.apache.druid.java.util.common.logger.Logger;
import org.apache.druid.java.util.emitter.service.ServiceEmitter;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.pipeline.Operator.OperatorDefn;
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

  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children,
        FragmentContext context) {
      Preconditions.checkArgument(children.size() == 1);
      return new MetricsOperator((Defn) defn, children.get(0));
    }
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends SingleChildDefn
  {
    private final Timer waitTimer = Timer.createStarted();
    private final ServiceEmitter emitter;
    private final String segmentIdString;
    private final QueryMetrics<?> queryMetrics;

    public Defn(
            final ServiceEmitter emitter,
            final String segmentIdString,
            final QueryMetrics<?> queryMetrics,
            final OperatorDefn child
    )
    {
      this.emitter = emitter;
      this.segmentIdString = segmentIdString;
      this.queryMetrics = queryMetrics;
      this.child = child;
    }
  }

  private final Defn defn;
  private final Operator child;
  private final Timer runTimer = Timer.create();
  private State state = State.START;

  public MetricsOperator(Defn defn, Operator child)
  {
    this.defn = defn;
    this.child = child;
  }

  @Override
  public void start() {
    if (state != State.START) {
      return;
    }
    state = State.RUN;
    runTimer.start();
    defn.queryMetrics.segment(defn.segmentIdString);
    child.start();
  }

  @Override
  public boolean hasNext() {
    return child.hasNext();
  }

  @Override
  public Object next() {
    return child.next();
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
    defn.queryMetrics.reportSegmentTime(runTimer.get());
    defn.queryMetrics.reportWaitTime(defn.waitTimer.get() - runTimer.get());
    try {
      defn.queryMetrics.emit(defn.emitter);
    }
    catch (Exception e) {
      // Query should not fail, because of emitter failure. Swallowing the exception.
      log.error("Failure while trying to emit [%s] with stacktrace [%s]", defn.emitter.toString(), e);
    }
  }
}
