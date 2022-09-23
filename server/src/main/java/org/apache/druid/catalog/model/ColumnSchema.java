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

package org.apache.druid.catalog.model;

import org.apache.druid.segment.column.ColumnType;

/**
 * Simple representation of the schema associated with a table
 * function so that formats (such as CSV) can infer the list of
 * columns from the SQL-provided schema.
 */
public class ColumnSchema
{
  private final String name;
  private final ColumnType type;

  public ColumnSchema(String name, ColumnType type)
  {
    this.name = name;
    this.type = type;
  }

  public String name()
  {
    return name;
  }

  public ColumnType type()
  {
    return type;
  }
}
