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
import org.apache.druid.catalog.specs.CatalogFieldDefn.StringFieldDefn;
import org.apache.druid.catalog.specs.CatalogFieldDefn.StringListDefn;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonProperty;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;

import java.io.File;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Definition of the input source conversions which converts from property
 * lists in table specs to subclasses of {@link InputSource}.
 */
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

  public static final List<JsonProperty> HTTP_SOURCE_FIELDS = Arrays.asList(
      new JsonProperty(
          "httpAuthenticationUsername",
          new StringFieldDefn("user")
      ),
      new JsonProperty(
          "httpAuthenticationPassword",
          new StringFieldDefn("password")
      ),
      new UriListProperty("uris", "uris"),
      new UriListProperty("uris", "uri")
  );

  public static final List<JsonProperty> LOCAL_SOURCE_FIELDS = Arrays.asList(
      new JsonProperty(
          "baseDir",
          new StringFieldDefn("baseDir")
      ),
      new JsonProperty(
          "filter",
          new StringFieldDefn("fileFilter")
      ),

      // Note that the LocalInputSource wants a list of File, but we define
      // the property as list of String with no conversion. As it turns out,
      // Jackson will do the string-to-File conversion for us. If we did try to
      // do it ourselves, the conversion would helpfully convert a relative path
      // to an absolute path relative to the current directory, which is not
      // helpful in this case.
      new JsonProperty(
          "files",
          new StringListDefn("files")
      )
  );

  public static final JsonSubclassConverter<InlineInputSource> INLINE_SOURCE_CONVERTER =
      new JsonSubclassConverter<>(
          InlineInputSource.TYPE_KEY,
          InlineInputSource.TYPE_KEY,
          INLINE_SOURCE_FIELDS,
          InlineInputSource.class
      );

  public static final JsonSubclassConverter<LocalInputSource> LOCAL_SOURCE_CONVERTER =
      new JsonSubclassConverter<>(
          LocalInputSource.TYPE_KEY,
          LocalInputSource.TYPE_KEY,
          LOCAL_SOURCE_FIELDS,
          LocalInputSource.class
      );

  public static final JsonSubclassConverter<HttpInputSource> HTTP_SOURCE_CONVERTER =
      new JsonSubclassConverter<>(
          HttpInputSource.TYPE_KEY,
          HttpInputSource.TYPE_KEY,
          HTTP_SOURCE_FIELDS,
          HttpInputSource.class
      );

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
