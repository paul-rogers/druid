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

import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlNameMatcher;
import org.apache.druid.sql.calcite.expression.SqlOperatorConversion;
import org.apache.druid.sql.calcite.planner.DruidOperatorRegistry.OperatorKey;
import org.apache.druid.sql.calcite.planner.convertlet.DruidConvertletTable;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

public class BaseOperatorTable
{
  private final Map<OperatorKey, SqlOperatorConversion> operatorConversions;

  protected BaseOperatorTable(
      Map<OperatorKey, SqlOperatorConversion> operatorConversions
  )
  {
    this.operatorConversions = operatorConversions;
  }

  public void lookupOperatorOverloads(
      final OperatorKey operatorKey,
      final List<SqlOperator> operatorList,
      final SqlNameMatcher nameMatcher
  )
  {
    final SqlOperatorConversion operatorConversion = operatorConversions.get(operatorKey);
    if (operatorConversion != null) {
      operatorList.add(operatorConversion.calciteOperator());
    }

    final SqlOperator convertletOperator = DruidOperatorRegistry.CONVERTLET_OPERATORS.get(operatorKey);
    if (convertletOperator != null) {
      operatorList.add(convertletOperator);
    }
  }

  public void populateOperatorList(List<SqlOperator> retVal)
  {
    for (SqlOperatorConversion operatorConversion : operatorConversions.values()) {
      retVal.add(operatorConversion.calciteOperator());
    }
    retVal.addAll(DruidConvertletTable.knownOperators());
  }


  @Nullable
  public SqlOperatorConversion lookupOperatorConversion(final SqlOperator operator)
  {
    final SqlOperatorConversion operatorConversion = operatorConversions.get(OperatorKey.of(operator));
    if (operatorConversion != null && operatorConversion.calciteOperator().equals(operator)) {
      return operatorConversion;
    } else {
      return null;
    }
  }
}
