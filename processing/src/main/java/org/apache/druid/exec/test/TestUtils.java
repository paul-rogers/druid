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

package org.apache.druid.exec.test;

import org.apache.druid.exec.batch.BatchType.BatchFormat;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.Batches;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.fragment.FragmentConverter;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.fragment.OperatorConverter;
import org.apache.druid.exec.plan.FragmentSpec;
import org.apache.druid.exec.plan.OperatorSpec;

import java.util.Collections;
import java.util.List;

public class TestUtils
{
  public static BatchBuilder builderFor(RowSchema schema, BatchFormat format)
  {
    return BatchBuilder.of(writerFor(schema, format, Integer.MAX_VALUE));
  }

  public static BatchWriter<?> writerFor(RowSchema schema, BatchFormat format, int limit)
  {
    return Batches.typeFor(format).newWriter(schema, limit);
  }

  /**
   * A simple fragment context for testing.
   */
  public static FragmentManager emptyFragment()
  {
    return new FragmentManager("dummy", new FragmentSpec(1, 1, 0, Collections.emptyList()));
  }

  /**
   * Fragment for testing that assumes the root is operator 1.
   */
  public static FragmentSpec simpleSpec(List<OperatorSpec> opSpecs)
  {
    return new FragmentSpec(1, 1, 1, opSpecs);
  }

  public static FragmentManager fragment(OperatorConverter converter, List<OperatorSpec> opSpecs)
  {
    return FragmentConverter.build(converter, "dummy", simpleSpec(opSpecs));
  }
}
