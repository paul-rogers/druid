package org.apache.druid.queryng.fragment;

import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.queryng.fragment.FragmentProfile.ProfileNode;

import java.util.List;
import java.util.Map.Entry;

public class ProfileVisualizer
{
  private final String INDENT = "| ";
  private final String METRIC_INDENT = "  ";

  private final FragmentProfile profile;
  private final StringBuilder buf = new StringBuilder();

  public ProfileVisualizer(FragmentProfile profile)
  {
    this.profile = profile;
  }

  public String render()
  {
    buf.setLength(0);
    for (ProfileNode root : profile.root()) {
      renderNode(0, root);
    }
    return buf.toString();
  }

  private void renderNode(int level, ProfileNode node)
  {
    String indent = StringUtils.repeat(INDENT, level);
    buf.append(indent)
       .append(node.operatorName())
       .append("\n");
    List<ProfileNode> children = node.children();
    int childCount = children == null ? 0 : children.size();
    String innerIndent = indent + StringUtils.repeat(INDENT, childCount);
    for (Entry<String, Long> entry : node.metrics().entrySet()) {
      buf.append(innerIndent)
         .append(METRIC_INDENT)
         .append(entry.getKey())
         .append(": ")
         .append(entry.getValue())
         .append("\n");
    }
    if (children == null) {
      return;
    }
    buf.append(innerIndent).append("\n");
    for (int i = childCount - 1; i >= 0; i--) {
      renderNode(level + i, children.get(i));
    }
  }
}
