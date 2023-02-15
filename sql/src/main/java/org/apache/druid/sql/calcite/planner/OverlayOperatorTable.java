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

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlSyntax;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.druid.sql.calcite.aggregation.SqlAggregator;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidOperatorRegistry.OperatorKey;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class OverlayOperatorTable implements SqlOperatorTable
{
  final BaseOperatorTable baseTable;
  final Map<OperatorKey, SqlAggregator> aggregators;

  protected OverlayOperatorTable(
      final BaseOperatorTable baseTable,
      final Map<OperatorKey, SqlAggregator> aggregators
  )
  {
    this.baseTable = baseTable;
    this.aggregators = aggregators;
  }

  @Override
  public void lookupOperatorOverloads(
      final SqlIdentifier opName,
      final SqlFunctionCategory category,
      final SqlSyntax syntax,
      final List<SqlOperator> operatorList,
      final SqlNameMatcher nameMatcher
  )
  {
    if (opName == null || opName.names.size() != 1) {
      return;
    }

    final OperatorKey operatorKey = OperatorKey.of(opName.getSimple(), syntax);
    final SqlAggregator aggregator = aggregators.get(operatorKey);
    if (aggregator != null) {
      operatorList.add(aggregator.calciteFunction());
    }
    baseTable.lookupOperatorOverloads(operatorKey, operatorList, nameMatcher);
  }


  @Override
  public List<SqlOperator> getOperatorList()
  {
    final List<SqlOperator> retVal = new ArrayList<>();
    for (SqlAggregator aggregator : aggregators.values()) {
      retVal.add(aggregator.calciteFunction());
    }
    baseTable.populateOperatorList(retVal);
    return retVal;
  }


  @Nullable
  public SqlAggregator lookupAggregator(final SqlAggFunction aggFunction)
  {
    final SqlAggregator sqlAggregator = aggregators.get(OperatorKey.of(aggFunction));
    if (sqlAggregator != null && sqlAggregator.calciteFunction().equals(aggFunction)) {
      return sqlAggregator;
    } else {
      return null;
    }
  }

  @Nullable
  public SqlOperatorConversion lookupOperatorConversion(final SqlOperator operator)
  {
    return baseTable.lookupOperatorConversion(operator);
  }
}
