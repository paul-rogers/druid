package org.apache.druid.exec.operator;

import java.util.List;

public interface OperatorSpec
{
  int id();
  List<OperatorSpec> children();
}
