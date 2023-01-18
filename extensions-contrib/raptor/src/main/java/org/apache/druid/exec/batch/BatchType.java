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

package org.apache.druid.exec.batch;

/**
 * Describes the layout and capabilities of an operator branch.
 * Creates readers and writers for the batch type, allowing operators to
 * work generically against a variety of batch formats.
 */
public interface BatchType
{
  enum BatchFormat
  {
    OBJECT_ARRAY,
    MAP,
    SCAN_OBJECT_ARRAY,
    SCAN_MAP,
    UNKNOWN
  }

  BatchFormat format();
  boolean canWrite();
  boolean canSeek();
  boolean canSort();
  BatchSchema batchSchema(RowSchema schema);
  Batch newBatch(RowSchema schema);
  BatchReader newReader(RowSchema schema);
  BatchWriter<?> newWriter(RowSchema schema, int sizeLimit);
  void bindReader(BatchReader reader, Object data);
  int sizeOf(Object data);
  boolean canDirectCopyFrom(BatchType otherType);
}
