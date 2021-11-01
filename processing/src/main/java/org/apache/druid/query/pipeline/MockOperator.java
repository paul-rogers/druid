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

import java.util.Iterator;
import java.util.function.Function;

import org.apache.druid.java.util.common.ISE;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContext;
import org.apache.druid.query.pipeline.Operator.IterableOperator;

import com.google.common.base.Preconditions;

public class MockOperator implements IterableOperator
{
  public enum Type
  {
    STRING, INT
  }

  public final Type type;
  public final int targetCount;
  private final Function<Integer,Object> generator;
  private int rowPosn = 0;
  public State state = State.START;


  public MockOperator(int rowCount, Type type) {
    this.type = type;
    this.targetCount = rowCount;
    switch(type)
    {
    case STRING:
      this.generator = rid -> "Mock row " + Integer.toString(rid);
      break;
    case INT:
      this.generator = rid -> rid;
      break;
     default:
      throw new ISE("Unknown type");
    }
  }

  @Override
  public Iterator<Object> open(FragmentContext context)
  {
    Preconditions.checkState(state == State.START);
    state = State.RUN;
    return this;
  }

  @Override
  public boolean hasNext()
  {
    return rowPosn < targetCount;
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
