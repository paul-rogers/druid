package org.apache.druid.queryng.fragment;

import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.List;
import java.util.Map;

public class FragmentProfile
{
  public static class ProfileNode
  {
    private final OperatorProfile operatorProfile;
    private final List<ProfileNode> children;

    protected ProfileNode(
        final OperatorProfile operatorProfile,
        final List<ProfileNode> children)
    {
      this.operatorProfile = operatorProfile;
      this.children = children == null || children.isEmpty() ? null : children;
    }

    public String operatorName()
    {
      return operatorProfile.operatorName();
    }

    public Map<String, Long> metrics()
    {
      return operatorProfile.metrics();
    }

    public List<ProfileNode> children()
    {
      return children;
    }
  }

  private final List<ProfileNode> roots;

  protected FragmentProfile(final List<ProfileNode> roots)
  {
    this.roots = roots;
  }

  public List<ProfileNode> root()
  {
    return roots;
  }
}
