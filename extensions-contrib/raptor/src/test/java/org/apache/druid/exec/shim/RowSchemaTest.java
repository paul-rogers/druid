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

package org.apache.druid.exec.shim;

import org.apache.druid.exec.batch.RowSchema;
import org.apache.druid.exec.batch.RowSchema.ColumnSchema;
import org.apache.druid.exec.batch.RowSchemaImpl;
import org.apache.druid.exec.util.SchemaBuilder;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.segment.column.ColumnType;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;

public class RowSchemaTest
{
  @Test
  public void testEmpty()
  {
    RowSchema schema = RowSchemaImpl.EMPTY_SCHEMA;
    assertEquals(0, schema.size());
    assertNull(schema.column("foo"));
    assertEquals(-1, schema.ordinal("foo"));
    assertThrows(IndexOutOfBoundsException.class, () -> schema.column(0));
  }

  @Test
  public void testBasics()
  {
    RowSchema schema = new SchemaBuilder()
        .scalar("foo", ColumnType.STRING)
        .scalar("bar", ColumnType.LONG)
        .build();
    assertEquals(2, schema.size());
    assertEquals(0, schema.ordinal("foo"));
    assertEquals(1, schema.ordinal("bar"));
    ColumnSchema col = schema.column(0);
    assertEquals("foo", col.name());
    assertEquals(ColumnType.STRING, col.type());
    assertSame(col, schema.column("foo"));
    col = schema.column(1);
    assertEquals("bar", col.name());
    assertEquals(ColumnType.LONG, col.type());
    assertSame(col, schema.column("bar"));
  }

  @Test
  public void testDuplicate()
  {
    SchemaBuilder builder = new SchemaBuilder()
        .scalar("foo", ColumnType.STRING)
        .scalar("foo", ColumnType.LONG);
    assertThrows(ISE.class, () -> builder.build());
  }

  @Test
  public void testEquals()
  {
    RowSchema schema1 = new SchemaBuilder()
        .scalar("foo", ColumnType.STRING)
        .scalar("bar", ColumnType.LONG)
        .build();
    RowSchema schema2 = new SchemaBuilder()
        .scalar("foo", ColumnType.STRING)
        .scalar("bar", ColumnType.LONG)
        .build();
    RowSchema schema3 = new SchemaBuilder()
        .scalar("foo", ColumnType.DOUBLE)
        .scalar("bar", ColumnType.LONG)
        .build();
    RowSchema schema4 = new SchemaBuilder()
        .scalar("foo", ColumnType.STRING)
        .scalar("bar", ColumnType.LONG)
        .scalar("mumble", null)
        .build();
    assertEquals(schema1, schema1);
    assertEquals(schema1, schema2);
    assertNotEquals(schema1, schema3);
    assertNotEquals(schema1, schema4);
  }
}
