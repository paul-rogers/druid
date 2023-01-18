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

package org.apache.druid.queryng.operators.scan;

import org.apache.druid.queryng.fragment.FragmentContext;
import org.apache.druid.queryng.operators.MappingOperator;
import org.apache.druid.queryng.operators.Operator;
import org.apache.druid.queryng.operators.OperatorProfile;

import java.util.List;
import java.util.Map;

/**
 * Converts individual scan query rows with the
 * {@link org.apache.druid.query.scan.ScanQuery.ResultFormat#RESULT_FORMAT_LIST
 * ResultFormat.RESULT_FORMAT_LIST} format into an object array with fields
 * in the order given by the output schema.
 *
 * @see {@link org.apache.druid.query.scan.ScanQueryQueryToolChest#resultsAsArrays
 * ScanQueryQueryToolChest.resultsAsArrays}
 */
public class ScanMapListToArrayOperator extends MappingOperator<Map<String, Object>, Object[]>
{
  private final List<String> fields;
  private int rowCount;

  public ScanMapListToArrayOperator(
      final FragmentContext context,
      final Operator<Map<String, Object>> input,
      final List<String> fields
  )
  {
    super(context, input);
    this.fields = fields;
  }

  @Override
  public Object[] next() throws EofException
  {
    Map<String, Object> row = inputIter.next();
    final Object[] rowArray = new Object[fields.size()];
    for (int i = 0; i < fields.size(); i++) {
      rowArray[i] = row.get(fields.get(i));
    }
    rowCount++;
    return rowArray;
  }

  @Override
  public void close(boolean cascade)
  {
    if (state == State.RUN) {
      OperatorProfile profile = new OperatorProfile("map-list-to-array");
      profile.add(OperatorProfile.ROW_COUNT_METRIC, rowCount);
      context.updateProfile(this, profile);
    }
    super.close(cascade);
  }
}
