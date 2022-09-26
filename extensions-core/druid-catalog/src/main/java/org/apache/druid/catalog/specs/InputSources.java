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

package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.druid.catalog.specs.CatalogFieldDefn.StringFieldDefn;
import org.apache.druid.catalog.specs.CatalogFieldDefn.StringListDefn;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonProperty;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverterImpl;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.PasswordProvider;
import org.apache.druid.utils.CollectionUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Definition of the input source conversions which converts from property
 * lists in table specs to subclasses of {@link InputSource}.
 */
// Suppressing deprecation warnings because there is nothing we can do
// about the deprecated input to the HTTP input source.
@SuppressWarnings("deprecation")
public class InputSources
{
  public static final List<JsonProperty> INLINE_SOURCE_FIELDS = Arrays.asList(
      new JsonProperty(
          "data",
          new StringFieldDefn("data")
      )
  );

  @SuppressWarnings("unchecked")
  public static List<URI> convertUriList(Object value)
  {
    if (value == null) {
      return null;
    }
    List<String> list;
    try {
      list = (List<String>) value;
    }
    catch (ClassCastException e) {
      throw new IAE("Value [%s] must be a list of strings", value);
    }
    List<URI> uris = new ArrayList<>();
    for (String strValue : list) {
      try {
        uris.add(new URI(strValue));
      }
      catch (URISyntaxException e) {
        throw new IAE(StringUtils.format("Argument [%s] is not a valid URI", value));
      }
    }
    return uris;
  }

  public static class UrisFieldDefn extends StringListDefn
  {
    public UrisFieldDefn(String name)
    {
      super(name);
    }

    @Override
    public void validate(Object value, ObjectMapper jsonMapper)
    {
      if (value == null) {
        return;
      }

      // Called just for the side effects
      convertUriList(value);
    }
  }

  public static class UriListProperty extends JsonProperty
  {
    public UriListProperty(String jsonKey, String propertyKey)
    {
      super(jsonKey, new UrisFieldDefn(propertyKey));
    }

    @Override
    public Object encodeToJava(Object value)
    {
      if (value == null) {
        return null;
      }
      return convertUriList(value);
    }
  }

  public static final String USER_PROPERTY = "user";
  public static final String PASSWORD_PROPERTY = "password";
  public static final String URIS_PROPERTY = "uris";
  public static final String ALLOWED_PROTOCOLS_PROPERTY = "allowedProtocols";

  /**
   * Hand-crafted converter because of the two non-simple objects required by
   * the constructor.
   */
  public static class HttpSourceConverter implements JsonSubclassConverter<HttpInputSource>
  {
    @Override
    public HttpInputSource convert(ResolvedTable table)
    {
      String user = table.stringProperty(USER_PROPERTY);
      String password = table.stringProperty(PASSWORD_PROPERTY);
      List<URI> uris = convertUriList(table.stringListProperty(URIS_PROPERTY));
      List<String> protocols = table.stringListProperty(ALLOWED_PROTOCOLS_PROPERTY);

      HttpInputSourceConfig config = new HttpInputSourceConfig(
          protocols == null ? null : new HashSet<>(protocols)
      );
      PasswordProvider passwordProvider = Strings.isNullOrEmpty(password)
          ? null
          : new DefaultPasswordProvider(password);

      return new HttpInputSource(uris, user, passwordProvider, config);
    }

    @Override
    public String typePropertyValue()
    {
      return LocalInputSource.TYPE_KEY;
    }

    @Override
    public HttpInputSource convert(ResolvedTable table, String jsonTypeFieldName)
    {
      return convert(table);
    }
  }

  @SuppressWarnings("unchecked")
  public static List<File> convertFileList(Object value)
  {
    if (value == null) {
      return null;
    }
    List<String> list;
    try {
      list = (List<String>) value;
    }
    catch (ClassCastException e) {
      throw new IAE("Value [%s] must be a list of strings", value);
    }
    return list
        .stream()
        .map(strValue -> new File(strValue))
        .collect(Collectors.toList());
  }

  // Local source fields since the convert is hand-crafted
  public static final String BASE_DIR_PROPERTY = "baseDir";

  // Note name "fileFilter", not "filter". These properties mix in with
  // others and "filter" is a bit too generic in that context.
  public static final String FILE_FILTER_PROPERTY = "fileFilter";
  public static final String FILES_PROPERTY = "files";

  /**
   * Hand-crafted converter to work around the the crazy-ass semantics of this class.
   * If we give a base directory, and explicitly state files, we must
   * also provide a file filter which presumably matches the very files
   * we list. Take pitty on the user and provide a filter in this case.
   */
  public static class LocalInputSourceConverter implements JsonSubclassConverter<LocalInputSource>
  {
    @Override
    public LocalInputSource convert(ResolvedTable table)
    {
      String baseDir = table.stringProperty(BASE_DIR_PROPERTY);
      String filter = table.stringProperty(FILE_FILTER_PROPERTY);
      List<File> files = convertFileList(table.stringListProperty(FILES_PROPERTY));

      // Note the crazy-ass semantics of this class.
      // If we give a base directory, and explicitly state files, we must
      // also provide a file filter which presumably matches the very files
      // we list. Take pitty on the user and provide a filter in this case.

      if (baseDir != null && filter == null && !CollectionUtils.isNullOrEmpty(files)) {
        filter = "*";
      }
      File baseDirFile = baseDir == null ? null : new File(baseDir);
      return new LocalInputSource(baseDirFile, filter, files);
    }

    @Override
    public String typePropertyValue()
    {
      return LocalInputSource.TYPE_KEY;
    }

    @Override
    public LocalInputSource convert(ResolvedTable table, String jsonTypeFieldName)
    {
      return convert(table);
    }
  }

  public static final JsonSubclassConverter<InlineInputSource> INLINE_SOURCE_CONVERTER =
      new JsonSubclassConverterImpl<>(
          InlineInputSource.TYPE_KEY,
          InlineInputSource.TYPE_KEY,
          INLINE_SOURCE_FIELDS,
          InlineInputSource.class
      );

  public static final JsonSubclassConverter<LocalInputSource> LOCAL_SOURCE_CONVERTER =
      new LocalInputSourceConverter();

  public static final JsonSubclassConverter<HttpInputSource> HTTP_SOURCE_CONVERTER = new HttpSourceConverter();

  public static final CatalogFieldDefn<String> INPUT_SOURCE_FIELD = new StringFieldDefn("inputSource");

  /**
   * Converter for the set of input sources as a union with the type determined
   * by (key, value) pairs in both the property and JSON representations.
   */
  public static final JsonUnionConverter<InputSource> INPUT_SOURCE_CONVERTER =
      new JsonUnionConverter<>(
          InputSource.class.getSimpleName(),
          INPUT_SOURCE_FIELD.name(),
          "type",
          Arrays.asList(
              INLINE_SOURCE_CONVERTER,
              LOCAL_SOURCE_CONVERTER,
              HTTP_SOURCE_CONVERTER
          )
      );
}
