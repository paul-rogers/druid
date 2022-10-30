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

package org.apache.druid.exec.internalSort;

import com.google.common.base.Preconditions;
import org.apache.druid.exec.fragment.FragmentContext;
import org.apache.druid.exec.operator.BatchOperator;
import org.apache.druid.exec.operator.Operator;
import org.apache.druid.exec.operator.OperatorFactory;
import org.apache.druid.exec.plan.InternalSortOp;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.java.util.common.UOE;

import java.util.List;

public class InternalSortFactory implements OperatorFactory<Object>
{
  @Override
  public Class<? extends OperatorSpec> accepts()
  {
    return InternalSortOp.class;
  }

  @Override
  public Operator<Object> create(FragmentContext context, OperatorSpec plan, List<Operator<?>> children)
  {
    Preconditions.checkArgument(children.size() == 1);
    BatchOperator input = (BatchOperator) children.get(0);
    InternalSortOp sortOp = (InternalSortOp) plan;
    switch (sortOp.sortType()) {
      case ROW:
        return new RowInternalSortOperator(context, sortOp, input);
      default:
        throw new UOE(sortOp.sortType().name());
    }
  }
}
