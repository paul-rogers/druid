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

package org.apache.druid.catalog.model.table;

import java.util.List;

/**
 * Base implementation for a table function definition.
 *
 * @see {@link TableFunction}
 */
public abstract class BaseFunctionDefn implements TableFunction
{
  public static class Parameter implements ParameterDefn
  {
    private final String name;
    private final Class<?> type;
    private final boolean optional;

    public Parameter(String name, Class<?> type, boolean optional)
    {
      this.name = name;
      this.type = type;
      this.optional = optional;
    }

    @Override
    public String name()
    {
      return name;
    }

    @Override
    public Class<?> type()
    {
      return type;
    }

    @Override
    public boolean isOptional()
    {
      return optional;
    }
  }

  private final List<ParameterDefn> parameters;

  public BaseFunctionDefn(List<ParameterDefn> parameters)
  {
    this.parameters = parameters;
  }

  @Override
  public List<ParameterDefn> parameters()
  {
    return parameters;
  }
}
