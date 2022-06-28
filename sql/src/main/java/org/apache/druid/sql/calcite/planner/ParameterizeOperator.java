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

package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlInternalOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.Span;
import org.apache.calcite.sql.validate.SqlValidator;

import java.util.List;

public class ParameterizeOperator extends SqlInternalOperator
{
  public static final ParameterizeOperator PARAM = new ParameterizeOperator();

  ParameterizeOperator()
  {
    super("PARAMETERS", SqlKind.OTHER, MDX_PRECEDENCE);
  }

  public SqlNode createCall(SqlNode tableRef, List<SqlNode> paramList)
  {
    SqlNode[] argArray = new SqlNode[paramList.size()];
    // Not entirely valid to use this operator for two purposes. But, since
    // we're going to rewrite the clause, should be OK.
    SqlBasicCall args = new SqlBasicCall(this, paramList.toArray(argArray), Span.of(paramList).pos());
    // TODO Auto-generated method stub
    return createCall(Span.of(tableRef, args).pos(), ImmutableList.of(tableRef, args));
  }

  @Override
  public SqlNode rewriteCall(SqlValidator validator, SqlCall call)
  {
    return call;
  }
}
