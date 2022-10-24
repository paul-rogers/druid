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

package org.apache.druid.exec.batch.impl;

import org.apache.druid.exec.operator.Batch;
import org.apache.druid.exec.operator.BatchCapabilities;
import org.apache.druid.exec.operator.BatchReader;
import org.apache.druid.exec.operator.BatchWriter;
import org.apache.druid.exec.operator.RowSchema;

public class IndirectBatch implements Batch
{
  private final Batch base;
  private final int[] index;

  public IndirectBatch(final Batch base, final int[] index)
  {
    this.base = base;
    this.index = index;
  }

  @Override
  public RowSchema schema()
  {
    return base.schema();
  }

  @Override
  public int size()
  {
    return index.length;
  }

  @Override
  public BatchCapabilities capabilities()
  {
    return base.capabilities();
  }

  @Override
  public BatchReader newReader()
  {
    IndirectBatchReader reader = new IndirectBatchReader();
    reader.bind(base, index);
    return reader;
  }

  @Override
  public BatchReader bindReader(BatchReader reader)
  {
    if (reader == null || !(reader instanceof IndirectBatchReader)) {
      return newReader();
    }
    ((IndirectBatchReader) reader).bind(base, index);
    return reader;
  }

  @Override
  public BatchWriter newWriter()
  {
    return base.newWriter();
  }
}
