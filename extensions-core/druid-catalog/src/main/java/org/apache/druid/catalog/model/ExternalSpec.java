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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.DelimitedInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.JsonInputFormat;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.segment.column.RowSignature;
import org.apache.druid.sql.calcite.external.ColumnSchema;
import org.apache.druid.sql.calcite.external.ExternalDataSource;
import org.apache.druid.sql.calcite.table.DruidTable;
import org.apache.druid.sql.calcite.table.ExternalTable;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Represents the input source and format converted from SQL table
 * function arguments. Inner classes provide the conversion tools.
 */
public class ExternalSpec
{
  /**
   * Converts from a SQL argument list to an {@code ExternalSpec}.
   */
  public static class ExternalSpecConverter extends ModelConverter<ExternalSpec>
  {
    private static ObjectConverter[] COMPONENTS = {
        new InputSourceConverter(),
        new InputFormatConverter()
    };

    @Inject
    public ExternalSpecConverter(ObjectMapper objectMapper)
    {
      super(objectMapper, ExternalSpec.class, COMPONENTS);
    }

    @Override
    public ExternalSpec convertFromMap(Map<String, Object> jsonMap, List<ColumnSchema> columns)
    {
      List<?> cols = columns.stream()
          .map(col -> ImmutableMap.of("name", col.name(), "type", col.type()))
          .collect(Collectors.toList());
      jsonMap.put("signature", cols);
      ExternalSpec spec = super.convertFromMap(jsonMap, columns);
      if (spec.inputFormat == null) {
        throw new IAE("An input format is required.");
      }
      if (spec.inputSource == null) {
        throw new IAE("An input source is required.");
      }
      return spec;
    }
  }

  /**
   * Represents the {@code inputSource} object within the {@code ExternalSpec}.
   */
  public static class InputSourceConverter extends ObjectConverter
  {
    private static SubTypeConverter[] SUBTYPES = {
        new LocalInputSourceConverter(),
        new InlineInputSourceConverter(),
        new HttpInputSourceConverter()
    };

    public InputSourceConverter()
    {
      super("inputSource", "source", "type", SUBTYPES);
    }
  }

  /**
   * Represents the local input source subtype of the input source.
   */
  protected static class LocalInputSourceConverter extends SubTypeConverter
  {
    private static PropertyConverter[] PROPERTIES = {
        new PropertyConverter("baseDir", "baseDir", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("fileFilter", "filter", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("files", "files", PropertyConverter.VARCHAR_FILE_LIST_TYPE),
        new PropertyConverter("file", "files", PropertyConverter.VARCHAR_FILE_LIST_TYPE)
    };

    public LocalInputSourceConverter()
    {
      super(LocalInputSource.TYPE_KEY, LocalInputSource.TYPE_KEY, PROPERTIES);
    }
  }

  /**
   * Represents the local input source subtype of the input source.
   */
  protected static class InlineInputSourceConverter extends SubTypeConverter
  {
    private static PropertyConverter[] PROPERTIES = {
        new PropertyConverter("data", "data", PropertyConverter.VARCHAR_TYPE)
    };

    public InlineInputSourceConverter()
    {
      super(InlineInputSource.TYPE_KEY, InlineInputSource.TYPE_KEY, PROPERTIES);
    }
  }

  /**
   * Represents the HTTP source subtype of the input source.
   */
  protected static class HttpInputSourceConverter extends SubTypeConverter
  {
    private static PropertyConverter[] PROPERTIES = {
        new PropertyConverter("user", "httpAuthenticationUsername", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("password", "httpAuthenticationPassword", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("uris", "uris", PropertyConverter.VARCHAR_URI_LIST_TYPE),
        new PropertyConverter("uri", "uris", PropertyConverter.VARCHAR_URI_LIST_TYPE)
    };

    public HttpInputSourceConverter()
    {
      super(HttpInputSource.TYPE_KEY, HttpInputSource.TYPE_KEY, PROPERTIES);
    }
  }

  /**
   * Represents the {@code inputFormat} object within the {@code ExternalSpec}.
   */
  public static class InputFormatConverter extends ObjectConverter
  {
    private static SubTypeConverter[] SUBTYPES = {
        new CsvFormatConverter(),
        new DelimitedTextFormatConverter(),
        new JsonFormatConverter()
    };

    public InputFormatConverter()
    {
      super("inputFormat", "format", "type", SUBTYPES);
    }
  }

  protected abstract static class FlatTextFormatConverter extends SubTypeConverter
  {
    public FlatTextFormatConverter(String sqlValue, String jsonTypeValue, PropertyConverter[] properties)
    {
      super(sqlValue, jsonTypeValue, properties);
    }

    @Override
    public Map<String, Object> convert(Map<String, ModelArg> args, List<ColumnSchema> columns)
    {
      Map<String, Object> jsonMap = super.convert(args, columns);
      // hasHeaderRow is required, even though we don't infer headers.
      jsonMap.put("hasHeaderRow", false);
      // Column list is required. Infer from schema.
      List<String> cols = columns.stream().map(col -> col.name()).collect(Collectors.toList());
      jsonMap.put("columns", cols);
      return jsonMap;
    }
  }

  /**
   * Represents the CSV format subtype within the {@code inputFormat}.
   * CSV is not a simple mapping: we must fill in the {@code hasHeaderRow}
   * property, which we don't want to expose in SQL. In SQL, the user must
   * provide the schema, via the SQL "extend" clause. That schema is mapped
   * into a column list for the CSV format so that the user doesn't have to
   * repeat the list.
   */
  protected static class CsvFormatConverter extends FlatTextFormatConverter
  {
    // Excludes properties for inferring columns from the header
    // as that is not yet supported by the SQL ingest engine.
    private static PropertyConverter[] PROPERTIES = {
        new PropertyConverter("listDelimiter", "listDelimiter", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("skipRows", "skipHeaderRows", PropertyConverter.INT_TYPE)
    };

    public CsvFormatConverter()
    {
      super(CsvInputFormat.TYPE_KEY, CsvInputFormat.TYPE_KEY, PROPERTIES);
    }
  }

  /**
   * Represents the TSV format subtype within the {@code inputFormat}.
   * Similar to {@link CsvFormatConverter}. This format is called "tsv" to
   * be consistent with the native format, though it is actually TSV only with
   * the default values.
   */
  protected static class DelimitedTextFormatConverter extends FlatTextFormatConverter
  {
    // Excludes properties for inferring columns from the header
    // as that is not yet supported by the SQL ingest engine.
    private static PropertyConverter[] PROPERTIES = {
        new PropertyConverter("delimiter", "delimiter", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("listDelimiter", "listDelimiter", PropertyConverter.VARCHAR_TYPE),
        new PropertyConverter("skipRows", "skipHeaderRows", PropertyConverter.INT_TYPE)
    };

    public DelimitedTextFormatConverter()
    {
      super(DelimitedInputFormat.TYPE_KEY, CsvInputFormat.TYPE_KEY, PROPERTIES);
    }
  }

  protected static class JsonFormatConverter extends SubTypeConverter
  {
    private static PropertyConverter[] PROPERTIES = {
    };

    public JsonFormatConverter()
    {
      super(JsonInputFormat.TYPE_KEY, JsonInputFormat.TYPE_KEY, PROPERTIES);
    }
  }

  protected final InputSource inputSource;
  protected final InputFormat inputFormat;
  protected final RowSignature signature;

  @JsonCreator
  public ExternalSpec(
      @JsonProperty("inputSource") final InputSource inputSource,
      @JsonProperty("inputFormat") final InputFormat inputFormat,
      @JsonProperty("signature") final RowSignature signature)
  {
    this.inputSource = inputSource;
    this.inputFormat = inputFormat;
    this.signature = signature;
  }

  public InputSource inputSource()
  {
    return inputSource;
  }

  public InputFormat inputFormat()
  {
    return inputFormat;
  }

  public RowSignature signature()
  {
    return signature;
  }

  public DruidTable toDruidTable(ObjectMapper jsonMapper)
  {
    return toDruidTable(signature, jsonMapper);
  }

  public DruidTable toDruidTable(RowSignature sig, ObjectMapper jsonMapper)
  {
    return new ExternalTable(
        new ExternalDataSource(
            inputSource,
            inputFormat,
            sig
          ),
        sig,
        jsonMapper
    );
  }
}
