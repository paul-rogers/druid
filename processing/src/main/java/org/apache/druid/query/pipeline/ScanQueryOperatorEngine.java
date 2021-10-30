package org.apache.druid.query.pipeline;

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContextImpl;
import org.apache.druid.query.pipeline.ScanQueryOperator.Defn;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine2;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;

public class ScanQueryOperatorEngine implements ScanQueryEngine2
{
  @Override
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext
  )
  {
    Defn defn = new Defn(query, segment);
    ScanQueryOperator reader = new ScanQueryOperator(defn,
        new FragmentContextImpl(query.getId(), responseContext)
        );
    return Operators.toLoneSequence(reader);
  }
}
