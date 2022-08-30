package org.apache.druid.queryng.fragment;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.logger.Logger;

public class Fragments
{
  private static final Logger log = new Logger(Fragments.class);

  public static void logProfile(FragmentContext context)
  {
    if (!(context instanceof FragmentContextImpl)) {
      return;
    }
    FragmentContextImpl contextImpl = (FragmentContextImpl) context;
    ProfileVisualizer vis = new ProfileVisualizer(contextImpl.buildProfile());
    log.info(
        StringUtils.format(
            "Query [%s] profile:\n%s",
            context.queryId(),
            vis.render()
        )
     );
  }
}
