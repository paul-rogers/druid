package org.apache.druid.exec.factory;

import com.google.common.collect.ImmutableMap;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.internalSort.InternalSortFactory;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.operator.OperatorSpec;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OperatorConverter
{
  private final Map<Class<? extends OperatorSpec>, OperatorFactory> factories;

  public OperatorConverter()
  {
    ImmutableMap.Builder<Class<? extends OperatorSpec>, OperatorFactory> builder = ImmutableMap.builder();
    builder.put(InternalSortOp.class, new InternalSortFactory());
    factories = builder.build();
  }

  public Operator create(FragmentManager context, OperatorSpec plan, List<Operator> children)
  {
    OperatorFactory factory = factories.get(plan.getClass());
    if (factory == null) {
      throw new ISE("Operator plan %s has no registered factory", plan.getClass().getSimpleName());
    }
    Operator op = factory.create(context, plan, children);
    context.register(plan, op);
    return op;
  }

  public Operator createTree(FragmentManager context, OperatorSpec plan)
  {
    Operator root = createSubtree(context, plan);
    context.registerRoot(root);
    return root;
  }

  private Operator createSubtree(FragmentManager context, OperatorSpec plan)
  {
    List<OperatorSpec> childPlans = plan.children();
    List<Operator> children = new ArrayList<>();
    for (OperatorSpec childPlan : childPlans) {
      children.add(createSubtree(context, childPlan));
    }
    return create(context, plan, children);
  }
}
