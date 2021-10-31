package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.Operator.FragmentContextImpl;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine2;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;

public class ScanQueryOperatorEngine implements ScanQueryEngine2
{
  /**
   * Return an instance of the scan query operator in a form that mimics the
   * "classic" ScanQuery Engine so that the operator version can be "slotted into"
   * existing code.
   */
  public static ScanQueryEngine2 asEngine()
  {
    return new ScanQueryOperatorEngine();
  }

  @Override
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext
  )
  {
    ScanQueryOperator reader = new ScanQueryOperator(query, segment);
    return Operators.toSequence(reader,
        new FragmentContextImpl(query.getId(), responseContext));
  }
}
