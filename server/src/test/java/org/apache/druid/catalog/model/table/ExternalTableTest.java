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

import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.logger.Logger;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertThrows;

public class ExternalTableTest extends BaseExternTableTest
{
  private static final Logger LOG = new Logger(ExternalTableTest.class);

  private final TableDefnRegistry registry = new TableDefnRegistry(mapper);

  @Test
  public void testValidateEmptyTable()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo").build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateMissingSourceType()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of())
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateUnknownSourceType()
  {
    // Empty table: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(ImmutableMap.of("type", "unknown"))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateSourceOnly()
  {
    // Input source only: valid, assumes the format is given later
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .column("a", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateMissingFormatType()
  {
    // Input source only: valid, assumes the format is given later
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(ImmutableMap.of())
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateUnknownFormatType()
  {
    // Input source only: valid, assumes the format is given later
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(ImmutableMap.of("type", "unknown"))
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateSourceAndFormat()
  {
    // Format is given without columns: it is validated
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(formatToMap(format))
        .column("a", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void docExample()
  {
    CsvInputFormat format = new CsvInputFormat(
        Collections.singletonList("a"), ";", false, false, 0);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(toMap(new InlineInputSource("a\n")))
        .inputFormat(formatToMap(format))
        .description("Logs from Apache web servers")
        .column("timetamp", Columns.VARCHAR)
        .column("sendBytes", Columns.BIGINT)
        .build();
    LOG.info(table.spec().toString());
  }
}
