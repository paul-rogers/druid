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
 * Cursor for a possibly unbounded set of rows with a common schema and common
 * set of column readers, along with a way to iterate over those rows in a
 * single forward pass. A "cursor" differs from an iterator: a
 * reader does not deliver a row, but rather positions a set of column readers
 * on the current row.
 */
public interface RowCursor extends RowReader, RowSequencer
{
}
