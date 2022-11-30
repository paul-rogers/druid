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

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.TableMetadata;
import org.apache.druid.catalog.model.table.InputFormats.CsvFormatDefn;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.impl.CloudObjectLocation;
import org.apache.druid.data.input.impl.CsvInputFormat;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.HttpInputSourceConfig;
import org.apache.druid.data.input.s3.S3InputSource;
import org.apache.druid.data.input.s3.S3InputSourceDruidModule;
import org.apache.druid.data.input.s3.S3InputSourceTest;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.storage.s3.S3StorageDruidModule;
import org.junit.Before;
import org.junit.Test;

import javax.annotation.Nullable;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Test the catalog definition on top of the S3 input source. Here we assume that
 * the S3 input source itself works. That is, that the {@link S3InputSourceTest} tests
 * pass.
 * <p>
 * Setup is a bit tricky. This test follows {@link S3InputSourceTest} in using mocks. In fact, this
 * test uses the mocks defined in {@link S3InputSourceTest}. Jackson setup is also tricky: we need
 * to register the right subclasses and injectables. {@link S3InputSourceTest} provides no method
 * to do both, so we cobble that together here.
 */
public class S3InputSourceDefnTest
{
  private static final List<ColumnSpec> COLUMNS = Arrays.asList(
      new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, "x", Columns.VARCHAR, null),
      new ColumnSpec(ExternalTableDefn.EXTERNAL_COLUMN_TYPE, "y", Columns.BIGINT, null)
  );

  /**
   * Minimum JSON input sourc format.
   */
  private static final String CSV_FORMAT = "{\"type\": \"" + CsvInputFormat.TYPE_KEY + "\"}";

  /**
   * Object mapper created using the {@link S3InputSourceTest} version, which registers
   * injectables (but, sadly, not subclasses.)
   */
  private final ObjectMapper mapper = S3InputSourceTest.createS3ObjectMapper();

  /**
   * Create a catalog table definition registry with the S3 extension added.
   */
  private final TableDefnRegistry registry = new TableDefnRegistry(
      null,
      Collections.singletonList(new S3InputSourceDefn()),
      null,
      mapper
  );
  private final InputSourceDefn s3Defn = registry.inputSourceDefnFor(S3StorageDruidModule.SCHEME);

  /**
   * Finish up Jackson configuration: add the required S3 input source subtype.
   */
  @Before
  public void setup()
  {
    mapper.registerModules(new S3InputSourceDruidModule().getJacksonModules());
  }

  @Test
  public void testValidateEmptyInputSource()
  {
    // No data property: not valid
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + S3StorageDruidModule.SCHEME + "\"}")
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  /**
   * Utility method to handle the boilerplate required to create an S3 input source.
   * These tests care only about the properties which the catalog exposes.
   */
  private S3InputSource s3InputSource(
      @Nullable List<String> uris,
      @Nullable List<String> prefixes,
      @Nullable List<CloudObjectLocation> objects,
      @Nullable String objectGlob
  )
  {
    return new S3InputSource(
        S3InputSourceTest.SERVICE,
        S3InputSourceTest.SERVER_SIDE_ENCRYPTING_AMAZON_S3_BUILDER,
        S3InputSourceTest.INPUT_DATA_CONFIG,
        null,
        CatalogUtils.stringListToUriList(uris),
        CatalogUtils.stringListToUriList(prefixes),
        objects,
        objectGlob,
        null,
        null,
        null,
        null
    );
  }

  @Test
  public void testValidateNoFormat()
  {
    // No format: valid. Format can be provided at run time.
    S3InputSource s3InputSource = s3InputSource(
        Collections.singletonList("s3://foo/bar/file.csv"), null, null, null);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testValidateNoColumns() throws URISyntaxException
  {
    // If a format is provided, then columns must also be provided.
    S3InputSource s3InputSource = s3InputSource(
        Collections.singletonList("s3://foo/bar/file.csv"), null, null, null);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testValidateGood()
  {
    // Minimum possible configuration that passes validation.
    S3InputSource s3InputSource = s3InputSource(
        Collections.singletonList("s3://foo/bar/file.csv"), null, null, null);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testBucketOnly()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + S3StorageDruidModule.SCHEME + "\"}")
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    resolved.validate();
  }

  @Test
  public void testBucketAndUri()
  {
    S3InputSource s3InputSource = s3InputSource(
        Collections.singletonList("s3://foo/bar/file.csv"), null, null, null);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testBucketAndPrefix()
  {
    S3InputSource s3InputSource = s3InputSource(
        null, Collections.singletonList("s3://foo/bar/"), null, null);
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testBucketAndObject()
  {
    S3InputSource s3InputSource = s3InputSource(
        null,
        null,
        Collections.singletonList(new CloudObjectLocation("s3://foo", "bar/file.csv")),
        null
    );
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testBucketAndGlob()
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + S3StorageDruidModule.SCHEME
            + "\"objectGlob\": \"*.csv\"\"}")
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .build();
    ResolvedTable resolved = registry.resolve(table.spec());
    assertThrows(IAE.class, () -> resolved.validate());
  }

  @Test
  public void testAdHocParameters()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    assertTrue(hasParam(fn, S3InputSourceDefn.URIS_PARAMETER));
    assertTrue(hasParam(fn, S3InputSourceDefn.OBJECT_GLOB_PARAMETER));
    assertTrue(hasParam(fn, S3InputSourceDefn.PREFIXES_PARAMETER));
    assertTrue(hasParam(fn, S3InputSourceDefn.BUCKET_PARAMETER));
    assertTrue(hasParam(fn, S3InputSourceDefn.OBJECTS_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));
  }

  @Test
  public void testAdHocNoArgs()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));
  }

  @Test
  public void testAdHocUri()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.URIS_PARAMETER, "s3://foo/bar/file.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(CatalogUtils.stringToList("s3://foo/bar/file.csv")),
        s3InputSource.getUris()
    );

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply(args, Collections.emptyList(), mapper));
  }

  @Test
  public void testMultipleAdHocUris()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.URIS_PARAMETER, "s3://foo/bar/file1.csv, s3://foo/mumble/file2.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(Arrays.asList(
            "s3://foo/bar/file1.csv",
            "s3://foo/mumble/file2.csv"
        )),
        s3InputSource.getUris()
    );
  }

  @Test
  public void testAdHocBlankUri()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.URIS_PARAMETER, "   ");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));
  }

  @Test
  public void testAdHocUriWithGlob()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.URIS_PARAMETER, "s3://foo/bar/");
    args.put(S3InputSourceDefn.OBJECT_GLOB_PARAMETER, "*.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(CatalogUtils.stringToList("s3://foo/bar/")),
        s3InputSource.getUris()
    );
    assertEquals("*.csv", s3InputSource.getObjectGlob());
  }

  @Test
  public void testAdHocPrefix()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.PREFIXES_PARAMETER, "s3://foo/bar/data");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(CatalogUtils.stringToList("s3://foo/bar/data")),
        s3InputSource.getPrefixes()
    );

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply(args, Collections.emptyList(), mapper));
  }

  @Test
  public void testMultipleAdHocPrefixes()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.PREFIXES_PARAMETER, "s3://foo/bar/, s3://foo/mumble/");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(
        CatalogUtils.stringListToUriList(Arrays.asList(
            "s3://foo/bar/",
            "s3://foo/mumble/"
        )),
        s3InputSource.getPrefixes()
    );
  }

  @Test
  public void testAdHocObject()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.BUCKET_PARAMETER, "foo.com");
    args.put(S3InputSourceDefn.OBJECTS_PARAMETER, "bar/file.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(1, s3InputSource.getObjects().size());
    CloudObjectLocation obj = s3InputSource.getObjects().get(0);
    assertEquals("foo.com", obj.getBucket());
    assertEquals("bar/file.csv", obj.getPath());

    // But, it fails if there are no columns.
    assertThrows(IAE.class, () -> fn.apply(args, Collections.emptyList(), mapper));
  }

  @Test
  public void testAdHocObjectWithoutBucket()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.OBJECTS_PARAMETER, "bar/file.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));
  }

  @Test
  public void testAdHocBucketWithoutObjects()
  {
    TableFunction fn = s3Defn.adHocTableFn();
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.BUCKET_PARAMETER, "foo.com");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);

    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));
  }

  @Test
  public void testMultipleAdHocObjects()
  {
    TableFunction fn = s3Defn.adHocTableFn();

    // Convert to an external table. Must provide the URIs plus format and columns.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.BUCKET_PARAMETER, "foo.com");
    args.put(S3InputSourceDefn.OBJECTS_PARAMETER, "bar/file1.csv, mumble/file2.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(2, s3InputSource.getObjects().size());
    CloudObjectLocation obj = s3InputSource.getObjects().get(0);
    assertEquals("foo.com", obj.getBucket());
    assertEquals("bar/file1.csv", obj.getPath());obj = s3InputSource.getObjects().get(1);
    assertEquals("foo.com", obj.getBucket());
    assertEquals("mumble/file2.csv", obj.getPath());
  }

  @Test
  public void testFullTableSpecHappyPath() throws URISyntaxException
  {
    S3InputSource s3InputSource = s3InputSource(
        Arrays.asList("s3://foo/bar/", "s3://mumble/"), null, null, "*.csv");
    TableMetadata table = TableBuilder.external("foo")
        .inputSource(mapper, s3InputSource)
        .inputFormat(CSV_FORMAT)
        .column("x", Columns.VARCHAR)
        .column("y", Columns.BIGINT)
        .build();

    // Check validation
    table.validate();

    // Check registry
    ResolvedTable resolved = registry.resolve(table.spec());
    assertNotNull(resolved);

    // Convert to an external spec
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    ExternalTableSpec externSpec = externDefn.convert(resolved);
    assertEquals(s3InputSource, externSpec.inputSource);

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(fn.parameters().isEmpty());

    // Convert to an external table.
    externSpec = fn.apply(Collections.emptyMap(), Collections.emptyList(), mapper);
    assertEquals(s3InputSource, externSpec.inputSource);

    // But, it fails columns are provided since the table already has them.
    assertThrows(IAE.class, () -> fn.apply(Collections.emptyMap(), COLUMNS, mapper));
  }

  @Test
  public void testTableSpecWithBucketAndFormat() throws URISyntaxException
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + S3StorageDruidModule.SCHEME + "\"}")
        .inputFormat(CSV_FORMAT)
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .column("x", Columns.VARCHAR)
        .column("y", Columns.BIGINT)
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec fails, because the table is partial
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(hasParam(fn, S3InputSourceDefn.OBJECTS_PARAMETER));
    assertFalse(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));

    // Convert to an external table.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.OBJECTS_PARAMETER, "bar/file.csv");
    ExternalTableSpec externSpec = fn.apply(args, Collections.emptyList(), mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(1, s3InputSource.getObjects().size());
    CloudObjectLocation obj = s3InputSource.getObjects().get(0);
    assertEquals("foo.com", obj.getBucket());
    assertEquals("bar/file.csv", obj.getPath());

    // But, it fails columns are provided since the table already has them.
    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));

    // Also fails if the user omits the objects argument
    assertThrows(IAE.class, () -> fn.apply(Collections.emptyMap(), Collections.emptyList(), mapper));
  }

  @Test
  public void testTableSpecAsConnection() throws URISyntaxException
  {
    TableMetadata table = TableBuilder.external("foo")
        .inputSource("{\"type\": \"" + S3StorageDruidModule.SCHEME + "\"}")
        .property(S3InputSourceDefn.BUCKET_PROPERTY, "foo.com")
        .build();

    // Check validation
    table.validate();

    // Convert to an external spec fails, because the table is partial
    ResolvedTable resolved = registry.resolve(table.spec());
    ExternalTableDefn externDefn = (ExternalTableDefn) resolved.defn();
    assertThrows(IAE.class, () -> externDefn.convert(resolved));

    // Get the partial table function
    TableFunction fn = externDefn.tableFn(resolved);
    assertTrue(hasParam(fn, S3InputSourceDefn.OBJECTS_PARAMETER));
    assertTrue(hasParam(fn, FormattedInputSourceDefn.FORMAT_PARAMETER));

    // Convert to an external table.
    Map<String, Object> args = new HashMap<>();
    args.put(S3InputSourceDefn.OBJECTS_PARAMETER, "bar/file.csv");
    args.put(FormattedInputSourceDefn.FORMAT_PARAMETER, CsvFormatDefn.TYPE_KEY);
    ExternalTableSpec externSpec = fn.apply(args, COLUMNS, mapper);

    S3InputSource s3InputSource = (S3InputSource) externSpec.inputSource;
    assertEquals(1, s3InputSource.getObjects().size());
    CloudObjectLocation obj = s3InputSource.getObjects().get(0);
    assertEquals("foo.com", obj.getBucket());
    assertEquals("bar/file.csv", obj.getPath());
    assertTrue(externSpec.inputFormat instanceof CsvInputFormat);

    // But, it fails columns are not provided since the table does not have them.
    assertThrows(IAE.class, () -> fn.apply(args, Collections.emptyList(), mapper));

    // Also fails if the user omits the objects argument
    assertThrows(IAE.class, () -> fn.apply(Collections.emptyMap(), Collections.emptyList(), mapper));

    // Also fails if the user omits the format argument
    args.remove(FormattedInputSourceDefn.FORMAT_PARAMETER);
    assertThrows(IAE.class, () -> fn.apply(args, COLUMNS, mapper));
  }

  protected boolean hasParam(TableFunction fn, String key)
  {
    for (ParameterDefn param : fn.parameters()) {
      if (param.name().equals(key)) {
        return true;
      }
    }
    return false;
  }
}
