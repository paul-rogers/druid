package org.apache.druid.queryng.fragment;

import org.apache.druid.query.Query;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.queryng.config.QueryNGConfig;

public class TestFragmentContextFactory implements FragmentContextFactory
{
  private static final String ENABLED_KEY = QueryNGConfig.CONFIG_ROOT + ".enabled";
  private static final boolean enabled = Boolean.parseBoolean(System.getProperty(ENABLED_KEY));

  @Override
  public FragmentContext create(Query<?> query, ResponseContext responseContext)
  {
    if (!enabled) {
      return null;
    }
    if (!(query instanceof ScanQuery)) {
      return null;
    }
    return new FragmentContextImpl(query, responseContext);
  }
}
