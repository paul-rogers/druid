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

/**
 * Batch that represents a list of {@code Object} arrays where columns are represented
 * as values at an array index given by the associated schema.
 */
public class ObjectArrayBatch implements Batch
{
  public static final BatchCapabilities CAPABILITIES = new BatchCapabilitiesImpl(
      BatchFormat.OBJECT_ARRAY,
      true, // Can seek
      false // Can't sort
  );

  private final RowSchema schema;
  private final List<Object[]> batch;

  public ObjectArrayBatch(final RowSchema schema, final List<Object[]> batch)
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
    ObjectArrayListReader reader = new ObjectArrayListReader(schema);
    reader.bind(batch);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof ObjectArrayListReader)) {
      return newReader();
    }
    ((ObjectArrayListReader) reader).bind(batch);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return new ObjectArrayListWriter(schema);
  }
}
