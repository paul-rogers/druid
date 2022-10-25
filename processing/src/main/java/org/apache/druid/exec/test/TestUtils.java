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

import org.apache.druid.exec.fragment.FragmentConverter;
import org.apache.druid.exec.fragment.FragmentManager;
import org.apache.druid.exec.fragment.OperatorConverter;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;
import org.apache.druid.exec.plan.FragmentSpec;
import org.apache.druid.exec.plan.OperatorSpec;
import org.apache.druid.exec.shim.MapListWriter;
import org.apache.druid.exec.shim.ObjectArrayListWriter;
import org.apache.druid.exec.shim.ScanResultValueWriter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;

import java.util.Collections;
import java.util.List;

public class TestUtils
{
  public static BatchBuilder builderFor(RowSchema schema, BatchFormat format)
  {
    return BatchBuilder.of(writerFor(schema, format, Integer.MAX_VALUE));
  }

  public static BatchWriter writerFor(RowSchema schema, BatchFormat format, int limit)
  {
    switch (format) {
      case OBJECT_ARRAY:
        return new ObjectArrayListWriter(schema, limit);
      case MAP:
        return new MapListWriter(schema, limit);
      case SCAN_OBJECT_ARRAY:
        return new ScanResultValueWriter("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST, limit);
      case SCAN_MAP:
        return new ScanResultValueWriter("dummy", schema, ScanQuery.ResultFormat.RESULT_FORMAT_LIST, limit);
      default:
        throw new UOE("Invalid batch format");
    }
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
