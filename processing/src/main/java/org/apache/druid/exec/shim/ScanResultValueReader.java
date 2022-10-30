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

import org.apache.druid.exec.batch.BatchSchema;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanQuery.ResultFormat;
import org.apache.druid.query.scan.ScanResultValue;

/**
 * Batch reader for a {@code ScanQuery} {@code ScanResultValue}.
 * Delegates to the object array or map reader depending on the format
 * of this particular value.
 */
public class ScanResultValueReader extends DelegatingBatchReader
{
  private final ResultFormat format;
  private BatchReader delegate;

  public ScanResultValueReader(final BatchSchema factory, ScanQuery.ResultFormat format)
  {
    super(factory);
    this.format = format;
    this.delegate = ScanResultValueBatchType.baseType(format).newReader(factory.rowSchema());
  }

  @Override
  protected BatchReader delegate()
  {
    return delegate;
  }

  public void bind(ScanResultValue batch)
  {
    if (format == null) {
      return;
    }
    switch (format) {
      case RESULT_FORMAT_LIST:
        ((MapListReader) delegate).bind(batch.getRows());
        break;
      case RESULT_FORMAT_COMPACTED_LIST:
        ((ObjectArrayListReader) delegate).bind(batch.getRows());
        break;
      default:
        throw new UOE(format.name());
    }
  }

  public ScanQuery.ResultFormat format()
  {
    return format;
  }
}
