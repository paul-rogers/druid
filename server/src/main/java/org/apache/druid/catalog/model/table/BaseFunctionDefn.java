package org.apache.druid.catalog.model.table;

import java.util.List;

public abstract class BaseFunctionDefn implements TableFunction
{
  public static class Parameter implements ParameterDefn
  {
    private final String name;
    private final Class<?> type;
    private final boolean optional;

    public Parameter(String name, Class<?> type, boolean optional)
    {
      this.name = name;
      this.type = type;
      this.optional = optional;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Class<?> type()
    {
      return type;
    }

    @Override
    public boolean isOptional()
    {
      return optional;
    }
  }

  private final List<ParameterDefn> parameters;

  public BaseFunctionDefn(List<ParameterDefn> parameters)
  {
    this.parameters = parameters;
  }

  @Override
  public List<ParameterDefn> parameters()
  {
    return parameters;
  }
}
