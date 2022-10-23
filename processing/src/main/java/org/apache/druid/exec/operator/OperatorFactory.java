package org.apache.druid.exec.operator;

import org.apache.druid.exec.fragment.FragmentContext;

import java.util.List;

public interface OperatorFactory
{
  Operator create(FragmentContext context, OperatorSpec plan, List<Operator> children);
}
