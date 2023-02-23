package org.apache.druid.sql.calcite.planner;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

/**
 * Copy of Calcite's {@code LogicalAggregate} because Calcite's
 * is final and we can't extend it to add Druid-specific hacks.
 */
public class DruidLogicalAggregate extends Aggregate
{
  /**
   * Creates a LogicalAggregate.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster    Cluster that this relational expression belongs to
   * @param traitSet   Traits
   * @param input      Input relational expression
   * @param groupSet Bit set of grouping fields
   * @param groupSets Grouping sets, or null to use just {@code groupSet}
   * @param aggCalls Array of aggregates to compute, not null
   */
  public DruidLogicalAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls
  )
  {
    super(cluster, traitSet, input, groupSet, groupSets, aggCalls);
  }

  /** Creates a LogicalAggregate. */
  public static DruidLogicalAggregate create(
      final RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls
  )
  {
    return create_(input, groupSet, groupSets, aggCalls);
  }

  private static DruidLogicalAggregate create_(
      final RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls
  )
  {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new DruidLogicalAggregate(cluster, traitSet, input, groupSet,
        groupSets, aggCalls);
  }

  @Override
  public DruidLogicalAggregate copy(
      RelTraitSet traitSet,
      RelNode input,
      ImmutableBitSet groupSet,
      List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls
  )
  {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new DruidLogicalAggregate(getCluster(), traitSet, input,
        groupSet, groupSets, aggCalls);
  }

  @Override
  public RelNode accept(RelShuttle shuttle) {
    return shuttle.visit(this);
  }
}
