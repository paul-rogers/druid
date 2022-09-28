package org.apache.druid.catalog.specs;

import org.apache.druid.catalog.specs.table.ExternalSpec;

import java.util.Map;

public interface Parameterized
{
  public interface ParameterDefn
  {
    String name();
    Class<?> valueClass();
  }

  public static class ParameterImpl implements ParameterDefn
  {
    private final String name;
    private final Class<?> type;

    public ParameterImpl(final String name, final Class<?> type)
    {
      this.name = name;
      this.type = type;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Class<?> valueClass()
    {
      return type;
    }
  }

  Map<String, ParameterDefn> parameters();
  ExternalSpec applyParameters(ResolvedTable table, Map<String, Object> parameters);
}
