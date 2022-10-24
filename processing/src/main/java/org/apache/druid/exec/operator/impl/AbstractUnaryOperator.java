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

package org.apache.druid.exec.operator.impl;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.ResultIterator;

import java.util.List;

public abstract class AbstractUnaryOperator extends AbstractOperator
{
  protected final Operator input;
  protected ResultIterator inputIter;

  public AbstractUnaryOperator(FragmentContext context, List<Operator> children)
  {
    super(context);
    Preconditions.checkArgument(children.size() == 1);
    this.input = children.get(0);
  }

  public void openInput()
  {
    inputIter = input.open();
  }

  public void closeInput()
  {
    if (inputIter != null) {
      input.close(true);
    }
    inputIter = null;
  }
}
