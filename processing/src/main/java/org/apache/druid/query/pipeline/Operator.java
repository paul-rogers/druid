package org.apache.druid.query.pipeline;

import java.util.Collections;
import java.util.List;

public interface Operator<T>
{
  interface OperatorDefn
  {
    List<OperatorDefn> children();
  }

  public abstract class AbstractOperatorDefn implements OperatorDefn
  {
    @Override
    public List<OperatorDefn> children() {
      return Collections.emptyList();
    }
  }

  interface OperatorFactory<T>
  {
    Operator<T> build(OperatorDefn defn, List<Operator<?>> children);
  }

  void start();
  boolean next();
  T get();
  void close();
}
