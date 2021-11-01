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

import org.apache.druid.java.util.common.guava.Sequence;
import org.apache.druid.query.QueryContexts;
import org.apache.druid.query.context.ResponseContext;
import org.apache.druid.query.pipeline.FragmentRunner.FragmentContextImpl;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQueryEngine2;
import org.apache.druid.query.scan.ScanResultValue;
import org.apache.druid.segment.Segment;

public class ScanQueryOperatorEngine implements ScanQueryEngine2
{
  /**
   * Return an instance of the scan query operator in a form that mimics the
   * "classic" ScanQuery Engine so that the operator version can be "slotted into"
   * existing code.
   */
  public static ScanQueryEngine2 asEngine()
  {
    return new ScanQueryOperatorEngine();
  }

  @Override
  public Sequence<ScanResultValue> process(
      final ScanQuery query,
      final Segment segment,
      final ResponseContext responseContext
  )
  {
    ScanQueryOperator reader = new ScanQueryOperator(query, segment);
    return Operators.toSequence(reader,
        new FragmentContextImpl(
            query.getId(),
            QueryContexts.getTimeout(query, FragmentRunner.NO_TIMEOUT),
            responseContext));
  }
}
