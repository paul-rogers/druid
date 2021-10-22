package org.apache.druid.query.profile;

import com.google.common.base.Preconditions;
import org.apache.druid.query.profile.OperatorProfile.OpaqueOperatorProfile;

import java.util.ArrayList;
import java.util.List;

public class ProfileStackImpl implements ProfileStack
{
  private final List<OperatorProfile> stack = new ArrayList<>();
  private OperatorProfile root;

  @Override
  public void push(OperatorProfile profile)
  {
    if (stack.isEmpty()) {
      Preconditions.checkState(root == null);
      root = profile;
    } else {
      stack.get(stack.size() - 1).addChild(profile);
    }
    stack.add(profile);
  }

  @Override
  public void pop(OperatorProfile profile)
  {
    Preconditions.checkState(!stack.isEmpty());
    Preconditions.checkState(stack.remove(stack.size() - 1) == profile);
  }

  @Override
  public OperatorProfile root()
  {
    Preconditions.checkState(stack.isEmpty());
    return root == null ?  new OpaqueOperatorProfile() : root;
  }
}
