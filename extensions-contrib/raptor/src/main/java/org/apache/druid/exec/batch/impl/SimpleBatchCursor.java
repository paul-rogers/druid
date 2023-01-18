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

import org.apache.druid.exec.batch.BatchCursor;
import org.apache.druid.exec.batch.BatchPositioner;
import org.apache.druid.exec.batch.BatchReader;
import org.apache.druid.exec.batch.ColumnReaderProvider;

public class SimpleBatchCursor implements BatchCursor
{
  private final BatchReader reader;
  private final BatchPositioner positioner;

  public SimpleBatchCursor(final BatchReader reader, final BatchPositioner positioner)
  {
    this.reader = reader;
    this.positioner = positioner;
    this.positioner.bindReader(reader);
    this.reader.bindListener(positioner);
  }

  public SimpleBatchCursor(final BatchReader reader)
  {
    this(reader, new SimpleBatchPositioner());
  }

  @Override
  public BatchPositioner positioner()
  {
    return positioner;
  }

  @Override
  public BatchReader reader()
  {
    return reader;
  }

  @Override
  public ColumnReaderProvider columns()
  {
    return reader.columns();
  }
}
