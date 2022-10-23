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

package org.apache.druid.exec.operator;

/**
 * Generic description of a batch of data. A batch is <i>either</i>
 * writable or readable, but not both at the same time. See the
 * {@link BatchReader} and {@link BatchWriter} interfaces for the concrete
 * actions on a batch.
 */
public interface Batch
{
  BatchCapabilities capabilities();
  BatchReader newReader();
  BatchReader bindReader(BatchReader reader);
  BatchWriter newWriter();
}
