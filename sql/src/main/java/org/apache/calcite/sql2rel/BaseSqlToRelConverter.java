package org.apache.calcite.sql2rel;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.prepare.Prepare.CatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.druid.sql.calcite.planner.DruidLogicalAggregate;

import java.util.List;

public class BaseSqlToRelConverter extends SqlToRelConverter
{
  public BaseSqlToRelConverter(
      final ViewExpander viewExpander,
      final SqlValidator validator,
      final CatalogReader catalogReader,
      RelOptCluster cluster,
      final SqlRexConvertletTable convertletTable,
      final Config config
  )
  {
    super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
  }

//  @Override
//  protected RelNode createAggregate(Blackboard bb, ImmutableBitSet groupSet,
//      ImmutableList<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
//    return DruidLogicalAggregate.create(bb.root, groupSet, groupSets, aggCalls);
//  }
}
