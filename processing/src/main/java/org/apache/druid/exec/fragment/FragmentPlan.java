package org.apache.druid.exec.fragment;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.exec.plan.FragmentSpec;
import org.apache.druid.exec.plan.OperatorSpec;

import java.util.Map;

public class FragmentPlan
{
  private final FragmentSpec spec;
  private final Map<Integer, OperatorSpec> operators;

  public FragmentPlan(FragmentSpec spec)
  {
    this.spec = spec;
    ImmutableMap.Builder<Integer, OperatorSpec> builder = ImmutableMap.builder();
    for (OperatorSpec opSpec : spec.operators()) {
      builder.put(opSpec.id(), opSpec);
    }
    this.operators = builder.build();
  }

  public FragmentSpec spec()
  {
    return spec;
  }

  public OperatorSpec operator(int id)
  {
    return operators.get(id);
  }
}
