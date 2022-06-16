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

package org.apache.druid.catalog;

import org.apache.druid.catalog.TableMetadata.TableType;

import java.util.Set;

/**
 * Defines the set of schemas available in Druid and their properties.
 * Since Druid has a fixed set of schemas, this registry is currently
 * hard-coded. That will change if/when Druid allows user-defined
 * schemas.
 */
public interface SchemaRegistry
{
  interface SchemaDefn
  {
    String name();
    String securityResource();
    boolean writable();
    boolean accepts(TableSpec defn);
    TableType tableType();
  }

  SchemaDefn schema(String name);
  Set<String> names();
}
