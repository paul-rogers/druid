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

import org.apache.druid.exec.batch.RowSchema.ColumnSchema;

import java.util.List;

public interface ColumnReaderProvider
{
  public interface ScalarColumnReader
  {
    ColumnSchema schema();
    boolean isNull();
    String getString();
    long getLong();
    double getDouble();

    /**
     * Return the value of a complex object. This is not a generic
     * getter, for that use {@link #getValue()}.
     */
    Object getObject();

    /**
     * Return the value of any type, as a Java object. If the
     * column is null, returns {@code null}.
     */
    Object getValue();
  }

  RowSchema schema();
  ScalarColumnReader scalar(String name);
  ScalarColumnReader scalar(int ordinal);
  List<ScalarColumnReader> columns();
}
