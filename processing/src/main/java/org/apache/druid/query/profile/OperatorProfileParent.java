package org.apache.druid.query.profile;

public interface OperatorProfileParent
{
  void addChild(OperatorProfile profile);
}