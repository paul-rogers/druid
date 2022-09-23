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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.CatalogTest;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.ColumnType;
import org.apache.druid.segment.column.RowSignature;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

@Category(CatalogTest.class)
public class ExternConverterTest
{
  @Test
  public void testLocalInputSource()
  {
    SubTypeConverter converter = new ExternalSpec.LocalInputSourceConverter();

    {
      Map<String, Object> args = ImmutableMap.of("baseDir", "/tmp", "fileFilter", "*.csv");
      Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
      Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

      Map<String, Object> expected = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "filter",
          "*.csv"
      );
      assertEquals(expected, jsonMap);
    }

    {
      Map<String, Object> args = ImmutableMap.of("baseDir", "/tmp", "file", "foo.csv");
      Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
      Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

      Map<String, Object> expected = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "files",
          Collections.singletonList(new File("foo.csv"))
      );
      assertEquals(expected, jsonMap);
    }

    {
      Map<String, Object> args = ImmutableMap.of("baseDir", "/tmp", "files", "foo.csv,bar.csv");
      Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
      Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

      Map<String, Object> expected = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "files",
          Arrays.asList(new File("foo.csv"), new File("bar.csv"))
      );
      assertEquals(expected, jsonMap);
    }
  }

  @Test
  public void testHttpInputSource() throws URISyntaxException
  {
    SubTypeConverter converter = new ExternalSpec.HttpInputSourceConverter();

    {
      Map<String, Object> args = ImmutableMap.of(
          "uri",
          "http://foo.org/myFile.csv",
          "user",
          "bob",
          "password",
          "secret"
      );
      Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
      Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

      Map<String, Object> expected = ImmutableMap.of(
          "uris",
          Collections.singletonList(new URI("http://foo.org/myFile.csv")),
          "httpAuthenticationUsername",
          "bob",
          "httpAuthenticationPassword",
          "secret"
      );
      assertEquals(expected, jsonMap);
    }

    {
      Map<String, Object> args = ImmutableMap.of(
          "uris",
          "http://foo.org/myFile.csv,http://foo.org/yourFile.csv"
      );
      Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
      Map<String, Object> jsonMap = converter.convert(modelArgs, Collections.emptyList());

      Map<String, Object> expected = ImmutableMap.of(
          "uris",
          Arrays.asList(
              new URI("http://foo.org/myFile.csv"),
              new URI("http://foo.org/yourFile.csv"))
      );
      assertEquals(expected, jsonMap);
    }
  }

  @Test
  public void testCsvFormat()
  {
    SubTypeConverter converter = new ExternalSpec.CsvFormatConverter();
    Map<String, Object> args = ImmutableMap.of(
        "listDelimiter", ",", "skipRows", 1);
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    List<ColumnSchema> cols = Arrays.asList(
        new ColumnSchema("x", ColumnType.STRING),
        new ColumnSchema("y", ColumnType.LONG));
    Map<String, Object> jsonMap = converter.convert(modelArgs, cols);

    Map<String, Object> expected = ImmutableMap.of(
        "listDelimiter",
        ",",
        "skipHeaderRows",
        1,
        "hasHeaderRow",
        false,
        "columns",
        Arrays.asList("x", "y")
    );
    assertEquals(expected, jsonMap);
  }

  @Test
  public void testJsonFormat()
  {
    SubTypeConverter converter = new ExternalSpec.JsonFormatConverter();
    Map<String, Object> args = ImmutableMap.of();
    Map<String, ModelArg> modelArgs = ModelArg.convertArgs(args);
    List<ColumnSchema> cols = Arrays.asList(
        new ColumnSchema("x", ColumnType.STRING),
        new ColumnSchema("y", ColumnType.LONG));
    Map<String, Object> jsonMap = converter.convert(modelArgs, cols);

    Map<String, Object> expected = ImmutableMap.of();
    assertEquals(expected, jsonMap);
  }

  @Test
  public void testWholeSpec()
  {
    ModelConverter<ExternalSpec> converter = new ExternalSpec.ExternalSpecConverter(new ObjectMapper());
    Map<String, Object> sourceArgs = ImmutableMap.of(
        "source", "local", "baseDir", "/tmp", "fileFilter", "*.csv");
    Map<String, Object> formatArgs = ImmutableMap.of(
        "format", "csv", "listDelimiter", "|", "skipRows", 1);
    Map<String, Object> args = new HashMap<>(sourceArgs);
    args.putAll(formatArgs);

    List<ColumnSchema> cols = Arrays.asList(
        new ColumnSchema("x", ColumnType.STRING),
        new ColumnSchema("y", ColumnType.LONG));

    Map<String, Object> expected = ImmutableMap.of(
        "inputSource",
        ImmutableMap.of(
            "type",
            "local",
            "baseDir",
            "/tmp",
            "filter",
            "*.csv"
        ),
        "inputFormat",
        ImmutableMap.of(
            "type",
            "csv",
            "listDelimiter",
            "|",
            "skipHeaderRows",
            1,
            "hasHeaderRow",
            false,
            "columns",
            Arrays.asList("x", "y")
        )
    );
    assertEquals(expected, converter.convertToMap(args, cols));

    ExternalSpec spec = converter.convert(args, cols);
    assertTrue(spec.inputSource instanceof LocalInputSource);
    assertTrue(spec.inputFormat instanceof CsvInputFormat);
    LocalInputSource localSource = (LocalInputSource) spec.inputSource;
    assertEquals(new File("/tmp"), localSource.getBaseDir());
    assertEquals("*.csv", localSource.getFilter());
    CsvInputFormat csvFormat = (CsvInputFormat) spec.inputFormat;
    assertEquals("|", csvFormat.getListDelimiter());
    assertEquals(1, csvFormat.getSkipHeaderRows());
    assertEquals(2, csvFormat.getColumns().size());
    RowSignature sig = spec.signature();
    assertEquals(2, sig.size());
  }

  @Test
  public void testInvalidSpecs()
  {
    ModelConverter<ExternalSpec> converter = new ExternalSpec.ExternalSpecConverter(new ObjectMapper());

    // No columns
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "inline",
          "format",
          "csv",
          "data",
          "a\nc\n"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, Collections.emptyList()));
    }

    // No format
    List<ColumnSchema> oneCol = Collections.singletonList(new ColumnSchema("a", ColumnType.STRING));
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "inline",
          "data",
          "a\nc\n"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }

    // No source
    {
      Map<String, Object> props = ImmutableMap.of(
          "format",
          "csv"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }

    // Invalid source
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "inline",
          "foo",
          "csv",
          "data",
          "a\nc\n"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }

    // Invalid Format
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "inline",
          "format",
          "foo"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }

    // Missing local properties
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "local",
          "format",
          "json"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }

    // Missing HTTP properties
    {
      Map<String, Object> props = ImmutableMap.of(
          "source",
          "http",
          "format",
          "json"
      );
      assertThrows(IAE.class, () -> converter.validateProperties(props, oneCol));
    }
  }
}
