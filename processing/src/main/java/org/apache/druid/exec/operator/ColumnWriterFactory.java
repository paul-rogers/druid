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

import org.apache.druid.exec.operator.RowSchema.ColumnSchema;

public interface ColumnWriterFactory
{
  interface ScalarColumnWriter
  {
    ColumnSchema schema();
    void setNull();
    void setString(String value);
    void setLong(long value);
    void setDouble(double value);
    void setObject(Object value);

    /**
     * Set the value of any column type from an Object of that type.
     * This operation does conversions and is expensive. Use the other
     * methods when types are known.
     */
    void setValue(Object value);
  }

  RowSchema schema();
  ScalarColumnWriter scalar(String name);
  ScalarColumnWriter scalar(int ordinal);
}
