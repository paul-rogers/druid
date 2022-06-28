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

// CHECKSTYLE.OFF: PackageName - Must be in Calcite

package org.apache.calcite.jdbc;

import org.apache.calcite.schema.Schema;

/**
 * Simple schema that allows the schema itself to specify which
 * CalciteSchema to use. This, in turn, allows the schema to resolve
 * function (and other) names dynamically, rather than via cached tables.
 * <p>
 * Is in the Calcite name space because {@link SimpleCalciteSchema}
 * is protected.
 */
public class DruidSimpleRootSchema extends SimpleCalciteSchema
{
  public DruidSimpleRootSchema(CalciteSchema parent, Schema schema, String name)
  {
    super(parent, schema, name);
  }

  /**
   * Create a Druid root schema. Replaces the call to
   * {@code CalciteSchema.createRootSchema(false, false)}.
   *
   * @see CalciteSchema#createRootSchema(boolean, boolean)
   */
  public static CalciteSchema createRootSchema()
  {
    final Schema rootSchema = new CalciteConnectionImpl.RootSchema();
    return new DruidSimpleRootSchema(null, rootSchema, "");
  }

  @Override
  public CalciteSchema add(String name, Schema schema)
  {
    if (schema instanceof SchemaCreator) {
      final SchemaCreator schemaCreator = (SchemaCreator) schema;
      final CalciteSchema calciteSchema = schemaCreator.createSchema(this, name);
      subSchemaMap.put(name, calciteSchema);
      return calciteSchema;
    } else {
      return super.add(name, schema);
    }
  }
}
