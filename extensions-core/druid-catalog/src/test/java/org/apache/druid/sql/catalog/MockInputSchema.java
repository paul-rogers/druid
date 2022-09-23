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

package org.apache.druid.sql.catalog;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.schema.NamedSchema;
import org.apache.druid.sql.calcite.table.ExternalTable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class MockInputSchema extends AbstractSchema implements NamedSchema
{
  public static final String SCHEMA_NAME = "input";
  public static final String INPUT_RESOURCE = "INPUT";

  static {
    ResourceType.registerResourceType(INPUT_RESOURCE);
  }

  private final Map<String, Table> tables = new HashMap<>();

  public MockInputSchema(ObjectMapper jsonMapper)
  {
    InputSource inputSource = new InlineInputSource("a,b,1\nc,d,2\n");
    InputFormat inputFormat = new CsvInputFormat(
        Arrays.asList("x", "y", "z"),
        null,  // listDelimiter
        false, // hasHeaderRow
        false, // findColumnsFromHeader
        0      // skipHeaderRows
        );
    RowSignature signature = RowSignature.builder()
        .add("x", ColumnType.STRING)
        .add("y", ColumnType.STRING)
        .add("z", ColumnType.LONG)
        .build();
    tables.put(
        "inline",
        new ExternalTable(
              new ExternalDataSource(inputSource, inputFormat, signature),
              signature,
              jsonMapper
        )
    );
  }

  @Override
  public String getSchemaName()
  {
    return SCHEMA_NAME;
  }

  @Override
  public String getSchemaResourceType(String resourceName)
  {
    return INPUT_RESOURCE;
  }

  @Override
  public Schema getSchema()
  {
    return this;
  }

  @Override
  protected Map<String, Table> getTableMap()
  {
    return tables;
  }
}
