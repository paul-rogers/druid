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

import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.BatchType;
import org.apache.druid.exec.batch.BatchWriter;
import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.impl.AbstractBatchType;
import org.apache.druid.exec.util.TypeInference;
import org.apache.druid.java.util.common.UOE;
import org.apache.druid.query.scan.ScanQuery;
import org.apache.druid.query.scan.ScanResultValue;

import java.util.List;

public class ScanResultValueBatchType extends AbstractBatchType
{
  public static final ScanResultValueBatchType MAP_INSTANCE =
      new ScanResultValueBatchType(
          BatchFormat.SCAN_MAP,
          ScanQuery.ResultFormat.RESULT_FORMAT_LIST
      );
  public static final ScanResultValueBatchType ARRAY_INSTANCE =
      new ScanResultValueBatchType(
          BatchFormat.SCAN_OBJECT_ARRAY,
          ScanQuery.ResultFormat.RESULT_FORMAT_COMPACTED_LIST
      );

  private final ScanQuery.ResultFormat resultFormat;

  public ScanResultValueBatchType(BatchFormat format, ScanQuery.ResultFormat resultFormat)
  {
    super(
        format,
        true,  // Can seek
        false, // Can't sort
        true   // Can write
    );
    this.resultFormat = resultFormat;
  }

  public ScanQuery.ResultFormat resultFormat()
  {
    return resultFormat;
  }

  public static ScanResultValueBatchType typeFor(ScanQuery.ResultFormat resultFormat)
  {
    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        return MAP_INSTANCE;
      case RESULT_FORMAT_COMPACTED_LIST:
        return ARRAY_INSTANCE;
      default:
        throw new UOE("Result format not supported");
    }
  }

  public RowSchema inferSchema(ScanResultValue batch)
  {
    final List<String> columnNames = batch.getColumns();
    switch (resultFormat) {
      case RESULT_FORMAT_LIST:
        return TypeInference.inferMapSchema(batch.getRows(), columnNames);
      case RESULT_FORMAT_COMPACTED_LIST:
        return TypeInference.inferArraySchema(batch.getRows(), columnNames);
      default:
        throw new UOE("Result format not supported");
    }
  }

  public static BatchType baseType(ScanQuery.ResultFormat format)
  {
    switch (format) {
      case RESULT_FORMAT_LIST:
        return MapListBatchType.INSTANCE;
      case RESULT_FORMAT_COMPACTED_LIST:
        return ObjectArrayListBatchType.INSTANCE;
      default:
        throw new UOE("Result format not supported");
    }
  }

  @Override
  public BatchReader newReader(RowSchema schema)
  {
    return new ScanResultValueReader(batchSchema(schema), resultFormat);
  }

  @Override
  public BatchWriter<?> newWriter(RowSchema schema, int sizeLimit)
  {
    return ScanResultValueWriter.newWriter(batchSchema(schema), null, sizeLimit, resultFormat);
  }

  @Override
  public void bindReader(BatchReader reader, Object data)
  {
    ((ScanResultValueReader) reader).bind(cast(data));
  }

  private ScanResultValue cast(Object data)
  {
    return (ScanResultValue) data;
  }

  @Override
  public int sizeOf(Object data)
  {
    return cast(data).getRows().size();
  }

  @Override
  public boolean canDirectCopyFrom(BatchType sourceType)
  {
    if (sourceType instanceof ScanResultValueBatchType) {
      ScanResultValueBatchType source = (ScanResultValueBatchType) sourceType;
      return source.resultFormat == resultFormat;
    } else {
      return baseType(resultFormat).canDirectCopyFrom(sourceType);
    }
  }
}
