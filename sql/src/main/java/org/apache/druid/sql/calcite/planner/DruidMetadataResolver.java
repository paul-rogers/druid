package org.apache.druid.sql.calcite.planner;

import org.apache.druid.query.aggregation.AggregatorFactory;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;

import javax.annotation.Nullable;

public interface DruidMetadataResolver
{
  @Nullable
  SqlAggregator aggregatorForFactory(Class<? extends AggregatorFactory> factoryClass);
}
