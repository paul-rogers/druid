package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverterImpl;
import org.apache.druid.catalog.specs.table.InputSources;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.junit.Test;

import java.io.File;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class InputSourceTest
{
  private final ObjectMapper mapper = new ObjectMapper();

  @Test
  public void testInlineInputSource()
  {
    JsonSubclassConverter<InlineInputSource> converter = InputSources.INLINE_SOURCE_CONVERTER;

    {
      Map<String, Object> args = ImmutableMap.of(
          "data",
          "a\nc\n"
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      InlineInputSource expectedSource = new InlineInputSource("a\nc\n");
      assertEquals(expectedSource, converter.convert(table, "type"));
    }

    // Missing data
    {
      Map<String, Object> args = ImmutableMap.of();
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table, "type"));
    }
  }

  @Test
  public void testLocalInputSource()
  {
    JsonSubclassConverter<LocalInputSource> converter = InputSources.LOCAL_SOURCE_CONVERTER;

    {
      Map<String, Object> args = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "fileFilter",
          "*.csv"
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      LocalInputSource expectedSource = new LocalInputSource(
          new File("/tmp"),
          "*.csv",
          null
      );
      assertEquals(expectedSource, converter.convert(table, "type"));
    }

    // Verify that the converter patches the odd semantics of this class.
    // If we give a base directory, and explicitly state files, we must
    // also provide a file filter which presumably matches the very files
    // we list.
    {
      Map<String, Object> args = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "files",
          Collections.singletonList("foo.csv")
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      LocalInputSource expectedSource = new LocalInputSource(
          new File("/tmp"),
          "*",
          Collections.singletonList(new File("foo.csv"))
      );
      assertEquals(expectedSource, converter.convert(table, "type"));
    }
  }

  @Test
  public void testHttpInputSource()
  {
    JsonSubclassConverter<HttpInputSource> converter = InputSources.HTTP_SOURCE_CONVERTER;

    // All properties
    {
      List<String> uris = Collections.singletonList("http://foo.org/myFile.csv");
      Map<String, Object> args = ImmutableMap.of(
          "uris",
          uris,
          "user",
          "bob",
          "password",
          "secret",
          "allowedProtocols",
          Collections.singletonList("http")

      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      HttpInputSource expectedSource = new HttpInputSource(
          InputSources.convertUriList(uris),
          "bob",
          new DefaultPasswordProvider("secret"),
          new HttpInputSourceConfig(ImmutableSet.of("http"))
      );
      assertEquals(expectedSource, converter.convert(table));
    }

    // Minimal properties
    {
      List<String> uris = Arrays.asList("http://foo.org/myFile.csv", "http://foo.org/yourFile.csv");
      Map<String, Object> args = ImmutableMap.of(
          "uris",
          uris
      );

      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      HttpInputSource expectedSource = new HttpInputSource(
          InputSources.convertUriList(uris),
          null,
          null,
          new HttpInputSourceConfig(null)
      );
      assertEquals(expectedSource, converter.convert(table));
    }

    // Invalid properties
    {
      Map<String, Object> args = ImmutableMap.of(
          "uris",
          Collections.singletonList("bogus")
      );

      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      assertThrows(Exception.class, () -> converter.convert(table));
    }
  }
}
