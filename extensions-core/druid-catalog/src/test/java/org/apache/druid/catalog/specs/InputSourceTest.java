package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class InputSourceTest
{
  private final ObjectMapper mapper = new ObjectMapper();

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

      Map<String, Object> jsonMap = converter.gatherFields(table);
      Map<String, Object> expected = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "filter",
          "*.csv"
      );
      assertEquals(expected, jsonMap);

      LocalInputSource expectedSource = new LocalInputSource(
          new File("/tmp"),
          "*.csv",
          null
      );
      assertEquals(expectedSource, converter.convert(table, "type"));
    }

    // Note the crazy-ass semantics of this class.
    // If we give a base directory, and explicitly state files, we must
    // also provide a file filter which presumably matches the very files
    // we list.
    {
      Map<String, Object> args = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "fileFilter",
          "*.csv",
          "files",
          Collections.singletonList("foo.csv")
      );
      TableSpec spec = new TableSpec("type", args, null);
      ResolvedTable table = new ResolvedTable(null, spec, mapper);

      Map<String, Object> jsonMap = converter.gatherFields(table);
      Map<String, Object> expected = ImmutableMap.of(
          "baseDir",
          "/tmp",
          "filter",
          "*.csv",
          "files",
          Collections.singletonList("foo.csv")
      );
      assertEquals(expected, jsonMap);

      LocalInputSource expectedSource = new LocalInputSource(
          new File("/tmp"),
          "*.csv",
          Collections.singletonList(new File("foo.csv"))
      );
      assertEquals(expectedSource, converter.convert(table, "type"));
    }
  }

}
