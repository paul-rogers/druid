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

import org.apache.druid.exec.batch.impl.BatchCapabilitiesImpl;
import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchCapabilities.BatchFormat;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;

import java.util.List;
import java.util.Map;

/**
 * Wraps a list of Java maps as a batch. The maps are assumed to use the
 * keys and types defined in the schema. If a map entry is missing, then it is
 * assumed that column value for that row is null.
 */
public class MapListBatch implements Batch
{
  public static final BatchCapabilities CAPABILITIES = new BatchCapabilitiesImpl(
      BatchFormat.MAP,
      true, // Can seek
      false // Can't sort
  );

  private final RowSchema schema;
  private final List<Map<String, Object>> batch;

  public MapListBatch(final RowSchema schema, final List<Map<String, Object>> batch)
  {
    this.schema = schema;
    this.batch = batch;
  }

  @Override
  public RowSchema schema()
  {
    return schema;
  }

  @Override
  public int size()
  {
    return batch.size();
  }

  @Override
  public BatchCapabilities capabilities()
  {
    return CAPABILITIES;
  }

  @Override
  public BatchReader newReader()
  {
    MapListReader reader = new MapListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof MapListReader)) {
      return newReader();
    }
    ((MapListReader) reader).bind(batch);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return new MapListWriter(schema);
  }
}
