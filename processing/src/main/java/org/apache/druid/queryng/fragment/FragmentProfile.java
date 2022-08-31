package org.apache.druid.queryng.fragment;

import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.List;

public class FragmentProfile
{
  public static class ProfileNode
  {
    public final OperatorProfile profile;
    public final List<ProfileNode> children;

    protected ProfileNode(
        final OperatorProfile operatorProfile,
        final List<ProfileNode> children)
    {
      this.profile = operatorProfile;
      this.children = children == null || children.isEmpty() ? null : children;
    }
  }

  public final String queryId;
  public final long runTimeMs;
  public final Exception error;
  public final List<ProfileNode> roots;

  protected FragmentProfile(FragmentContextImpl context, final List<ProfileNode> roots)
  {
    this.queryId = context.queryId();
    this.runTimeMs = context.elapsedTimeMs();
    this.error = context.exception();
    this.roots = roots;
  }
}
