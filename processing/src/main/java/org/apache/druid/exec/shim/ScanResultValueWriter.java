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

package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.BatchFactory;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.List;

/**
 * Batch writer for a {@code ScanQuery} {@code ScanResultValue}.
 * Delegates to the object array or map writer depending on the format
 * of this particular value.
 */
public class ScanResultValueWriter extends DelegatingBatchWriter<ScanResultValue>
{
  private final String segmentId;
  private final List<String> columnNames;

  public ScanResultValueWriter(
      final BatchFactory factory,
      final String segmentId,
      final int sizeLimit,
      final BatchWriter<?> baseWriter
  )
  {
    super(factory, baseWriter);
    this.segmentId = segmentId;
    this.columnNames = factory.schema().columnNames();
  }

  public static ScanResultValueWriter newWriter(
      final BatchFactory factory,
      final String segmentId,
      final int sizeLimit,
      final ScanQuery.ResultFormat format
  )
  {
    switch (format) {
      case RESULT_FORMAT_LIST:
        return new ScanResultValueWriter(
            factory,
            segmentId,
            sizeLimit,
            MapListBatchType.INSTANCE.newWriter(factory.schema(), sizeLimit)
        );
      case RESULT_FORMAT_COMPACTED_LIST:
        return new ScanResultValueWriter(
            factory,
            segmentId,
            sizeLimit,
            ObjectArrayListBatchType.INSTANCE.newWriter(factory.schema(), sizeLimit)
        );
      default:
        throw new UOE("Result format not supported");
    }
  }

  @Override
  public ScanResultValue harvest()
  {
    return new ScanResultValue(
        segmentId,
        columnNames,
        ((ListWriter<?>) delegate).harvest()
    );
  }
}
