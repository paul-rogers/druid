/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.query.pipeline;

import com.google.common.base.Preconditions;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.FragmentRunner.OperatorRegistry;
import org.apache.druid.query.pipeline.Operator.IterableOperator;

import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

public class MockOperator implements IterableOperator
{
  public static final OperatorFactory FACTORY = new OperatorFactory()
  {
    @Override
    public Operator build(OperatorDefn defn, List<Operator> children, FragmentContext context)
    {
      Preconditions.checkArgument(children.isEmpty());
      Defn mockDefn = (Defn) defn;
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
  };

  public static void register(OperatorRegistry reg) {
    reg.register(Defn.class, FACTORY);
  }

  public static class Defn extends LeafDefn
  {
    public enum Type
    {
      STRING, INT
    }
    public final Type type;
    public final int rowCount;

    public Defn(int rowCount, Type type)
    {
      this.type = type;
      this.rowCount = rowCount;
    }
  }

  private final Defn defn;
  private final Function<Integer,Object> generator;
  private int rowPosn = 0;
  public State state = State.START;


  public MockOperator(Defn defn, Function<Integer,Object> gen) {
    this.defn = defn;
    this.generator = gen;
  }

  @Override
  public Iterator<Object> open()
  {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return rowPosn < defn.rowCount;
  }

  @Override
  public Object next()
  {
    return generator.apply(rowPosn++);
  }

  @Override
  public void close(boolean cascade)
  {
    state = State.CLOSED;
  }
}
