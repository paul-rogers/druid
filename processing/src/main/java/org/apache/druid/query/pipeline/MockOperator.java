package org.apache.druid.query.pipeline;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;

import java.util.List;
import java.util.function.Function;

public class MockOperator implements Operator<Object>
{
  public static class MockOperatorDef extends AbstractOperatorDefn
  {
    public enum Type
    {
      STRING, INT
    }
    public final Type type;
    public final int rowCount;

    public MockOperatorDef(int rowCount, Type type)
    {
      this.type = type;
      this.rowCount = rowCount;
    }
  }

  public static class MockOperatorFactory implements OperatorFactory<Object>
  {

    @Override
    public Operator<Object> build(OperatorDefn defn, List<Operator<?>> children)
    {
      Preconditions.checkArgument(children.isEmpty());
      MockOperatorDef mockDefn = (MockOperatorDef) defn;
      switch(mockDefn.type)
      {
      case STRING:
        return new MockOperator(mockDefn, rid -> "Mock row " + Integer.toString(rid));
      case INT:
        return new MockOperator(mockDefn, rid -> rid);
       default:
        throw new ISE("Unknown type");
      }
    }
  }

  private final MockOperatorDef defn;
  private final Function<Integer,Object> generator;
  private int rowPosn = -1;
  public boolean started;
  public boolean closed;


  public MockOperator(MockOperatorDef defn, Function<Integer,Object> gen) {
    this.defn = defn;
    this.generator = gen;
  }

  @Override
  public void start()
  {
    started = true;
  }

  @Override
  public boolean next()
  {
    return ++rowPosn < defn.rowCount;
  }

  @Override
  public Object get()
  {
    return generator.apply(rowPosn);
  }

  @Override
  public void close()
  {
    closed = true;
  }
}
