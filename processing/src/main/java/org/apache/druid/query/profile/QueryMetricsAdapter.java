package org.apache.druid.query.profile;

import org.apache.druid.query.Query;
import org.apache.druid.query.QueryMetrics;
import org.apache.druid.query.context.ResponseContext;

public interface QueryMetricsAdapter<QueryType extends Query<?>> extends QueryMetrics<QueryType>
{
  void pushProfile(OperatorProfile profile);
  
  static <QueryType extends Query<?>> QueryMetricsAdapter<QueryType> wrap(QueryMetrics<QueryType> in, ResponseContext context) {
    if (in == null) {
      return new QueryMetricsStub<QueryType>(context);
    } else {
      return new QueryMetricsShim<QueryType>(in, context);
    }
  }
  
  static void setProfile(QueryMetrics<?> metrics, OperatorProfile profile) {
    if (metrics == null) {
      return;
    }
    if (! (metrics instanceof QueryMetricsAdapter)) {
      return;
    }
    ((QueryMetricsAdapter<?>) metrics).pushProfile(profile);
  }
}
