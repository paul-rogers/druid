package org.apache.druid.query.profile;

public interface ProfileStack
{
  void push(OperatorProfile profile);
  void pop(OperatorProfile profile);
  OperatorProfile root();
}
