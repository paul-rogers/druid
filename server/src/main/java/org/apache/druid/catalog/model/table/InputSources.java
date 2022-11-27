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
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.TableDefnRegistry;
import org.apache.druid.catalog.model.table.BaseFunctionDefn.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.data.input.impl.InlineInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.metadata.DefaultPasswordProvider;
import org.apache.druid.metadata.EnvironmentVariablePasswordProvider;
import org.apache.druid.utils.CollectionUtils;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class InputSources
{
  /**
   * Base class for input source definitions.
   *
   * @see {@link FormattedInputSourceDefn} for the base class for (most) input formats
   * which take an input format.
   */
  public abstract static class BaseInputSourceDefn implements InputSourceDefn
  {
    /**
     * The "from-scratch" table function for this input source. The parameters
     * are those defined by the subclass, and the apply simply turns around and
     * asks the input source definition to do the conversion.
     */
    public class AdHocTableFunction extends BaseFunctionDefn
    {
      public AdHocTableFunction(List<ParameterDefn> parameters)
      {
        super(parameters);
      }

      @Override
      public ExternalTableSpec apply(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
      {
        return convertArgsToTable(args, columns, jsonMapper);
      }
    }

    /**
     * The "partial" table function that starts with a catalog external table spec, then
     * uses SQL function arguments to "complete" (i.e. fill in) the missing properties to
     * produce a complete table which is then converted to an external table which Calcite
     * can use.
     * <p>
     * The set of parameters depends on the input source and on whether or not the catalog
     * spec provides a format.
     */
    public class PartialTableFunction extends BaseFunctionDefn
    {
      private final ResolvedExternalTable table;

      public PartialTableFunction(final ResolvedExternalTable table, List<ParameterDefn> params)
      {
        super(params);
        this.table = table;
      }

      @Override
      public ExternalTableSpec apply(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
      {
        return convertCompletedTable(table, args, columns);
      }
    }

    /**
     * The one and only from-scratch table function for this input source. The
     * function is defined a bind time, not construction time, since it typically
     * needs visibility to the set of available input formats.
     */
    private AdHocTableFunction adHocTableFn;

    /**
     * Overridden by each subclass to return the input source class to be
     * used for JSON conversions.
     */
    protected abstract Class<? extends InputSource> inputSourceClass();

    @Override
    public void bind(TableDefnRegistry registry)
    {
      this.adHocTableFn = defineAdHocTableFunction();
    }

    @Override
    public void validate(ResolvedExternalTable table)
    {
      convertTableToSource(table);
    }

    /**
     * Overridden by each subclass to define the parameters needed by each
     * input source.
     */
    protected abstract AdHocTableFunction defineAdHocTableFunction();

    @Override
    public TableFunction adHocTableFn()
    {
      return adHocTableFn;
    }

    /**
     * Define a table "from scratch" using SQL function arguments.
     */
    protected ExternalTableSpec convertArgsToTable(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      auditTableProperties(args);
      return new ExternalTableSpec(
          convertArgsToSource(args, jsonMapper),
          convertArgsToFormat(args, columns, jsonMapper),
          Columns.convertSignature(columns)
      );
    }

    /**
     * Convert the input source using arguments to a "from scratch" table function.
     */
    protected InputSource convertArgsToSource(Map<String, Object> args, ObjectMapper jsonMapper)
    {
      Map<String, Object> jsonMap = new HashMap<>();
      convertArgsToSourceMap(jsonMap, args);
      return convertSource(jsonMap, jsonMapper);
    }

    /**
     * Convert SQL arguments to the corresponding "generic JSON" form in the given map.
     * The map will then be adjusted and converted to the actual input source.
     */
    protected abstract void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args);

    /**
     * Convert SQL arguments, and the column schema, to an input format, if required.
     */
    protected InputFormat convertArgsToFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      return null;
    }

    /**
     * Complete a partial table using the table function arguments and columns provided.
     * The arguments match the set of parameters used for the function. The columns are
     * provided if the SQL included an {@code EXTENDS} clause: the implementation should decide
     * if columns are required (or allowed) depending on whether the partial spec already
     * defines columns.
     *
     * @param table   the partial table spec, with input source and format parsed into a
     *                generic Java map
     * @param args    the argument values provided in the SQL table function call. The arguments
     *                use the Java types defined in the parameter definitions.
     * @param columns the set of columns (if any) from the SQL {@code EXTEND} clause
     *
     * @return an external table spec which Calcite can consume
     */
    protected abstract ExternalTableSpec convertCompletedTable(
        ResolvedExternalTable table,
        Map<String, Object> args,
        List<ColumnSpec> columns
    );

    @Override
    public ExternalTableSpec convertTable(ResolvedExternalTable table)
    {
      return new ExternalTableSpec(
          convertTableToSource(table),
          convertTableToFormat(table),
          Columns.convertSignature(table.resolvedTable().spec().columns())
      );
    }

    /**
     * Converts the input source given in a table spec. Since Druid input sources
     * were not designed for the use by the catalog or SQL, some cleanup is done to
     * simplify the parameters which the user provides.
     *
     * @param table the resolved external table spec
     * @return the input source converted from the spec
     */
    protected InputSource convertTableToSource(ResolvedExternalTable table)
    {
      return convertSource(
          new HashMap<>(table.sourceMap()),
          table.resolvedTable().jsonMapper()
      );
    }

    /**
     * Convert from a generic Java map to the target input source using the object
     * mapper provided. Translates Jackson errors into a generic unchecked error.
     */
    protected InputSource convertSource(
        final Map<String, Object> jsonMap,
        final ObjectMapper jsonMapper
    )
    {
      try {
        auditTableProperties(jsonMap);
        return jsonMapper.convertValue(jsonMap, inputSourceClass());
      }
      catch (Exception e) {
        throw new IAE(e, "Invalid input source specification");
      }
    }

    /**
     * Optional step to audit or adjust the input source properties prior to
     * conversion via Jackson. Changes are made directly in the {@code jsonMap}.
     */
    protected void auditTableProperties(Map<String, Object> jsonMap)
    {
    }

    /**
     * Convert the format spec, if any, to an input format.
     */
    protected abstract InputFormat convertTableToFormat(ResolvedExternalTable table);
  }

  /**
   * Base class for input formats that require an input format (which is most of them.)
   * By default, an input source supports all formats defined in the table registry, but
   * specific input sources can be more restrictive. The list of formats defines the list
   * of SQL function arguments available when defining a table from scratch.
   */
  public abstract static class FormattedInputSourceDefn extends BaseInputSourceDefn
  {
    public static final String FORMAT_PARAMETER = "format";

    private Map<String, InputFormatDefn> formats;

    @Override
    public void bind(TableDefnRegistry registry)
    {
      formats = registry.formats();
      super.bind(registry);
    }

    @Override
    protected AdHocTableFunction defineAdHocTableFunction()
    {
      List<ParameterDefn> fullTableParams = adHocTableFnParameters();
      List<ParameterDefn> allParams = addFormatParameters(fullTableParams);
      return new AdHocTableFunction(allParams);
    }

    /**
     * Overridden by subclasses to provide the list of table function parameters for
     * this specific input format. This list is combined with parameters for input
     * formats. The method is called only once per run.
     */
    protected abstract List<ParameterDefn> adHocTableFnParameters();

    /**
     * Add format properties to the base set, in the order of the formats,
     * in the order defined by the format. Allow same-named properties across
     * formats, as long as the types are the same.
     */
    protected List<ParameterDefn> addFormatParameters(
        final List<ParameterDefn> properties
    )
    {
      final List<ParameterDefn> toAdd = new ArrayList<>();
      final ParameterDefn formatProp = new Parameter(FORMAT_PARAMETER, String.class, false);
      toAdd.add(formatProp);
      final Map<String, ParameterDefn> formatProps = new HashMap<>();
      for (InputFormatDefn format : formats.values()) {
        for (ParameterDefn prop : format.parameters()) {
          final ParameterDefn existing = formatProps.putIfAbsent(prop.name(), prop);
          if (existing == null) {
            toAdd.add(prop);
          } else if (existing.type() != prop.type()) {
            throw new ISE(
                "Format %s, property %s of class %s conflicts with another format property of class %s",
                format.typeValue(),
                prop.name(),
                prop.type().getSimpleName(),
                existing.type().getSimpleName()
            );
          }
        }
      }
      return CatalogUtils.concatLists(properties, toAdd);
    }

    @Override
    protected InputFormat convertTableToFormat(ResolvedExternalTable table)
    {
      final String formatTag = CatalogUtils.getString(table.formatMap(), InputFormat.TYPE_PROPERTY);
      if (formatTag == null) {
        throw new IAE("%s property must be set", InputFormat.TYPE_PROPERTY);
      }
      final InputFormatDefn formatDefn = formats.get(formatTag);
      if (formatDefn == null) {
        throw new IAE(
            "Format type [%s] for property %s is not valid",
            formatTag,
            InputFormat.TYPE_PROPERTY
        );
      }
      return formatDefn.convertFromTable(table);
    }

    @Override
    protected InputFormat convertArgsToFormat(Map<String, Object> args, List<ColumnSpec> columns, ObjectMapper jsonMapper)
    {
      final String formatTag = CatalogUtils.getString(args, FORMAT_PARAMETER);
      if (formatTag == null) {
        throw new IAE("%s parameter must be set", FORMAT_PARAMETER);
      }
      final InputFormatDefn formatDefn = formats.get(formatTag);
      if (formatDefn == null) {
        throw new IAE(
            "Format type [%s] for property %s is not valid",
            formatTag,
            FORMAT_PARAMETER
        );
      }
      return formatDefn.convertFromArgs(args, columns, jsonMapper);
    }
  }

  /**
   * Describes an inline input source: one where the data is provided in the
   * table spec as a series of text lines. Since the data is provided, the input
   * format is required in the table spec: it cannot be provided at ingest time.
   * Primarily for testing.
   */
  public static class InlineInputSourceDefn extends FormattedInputSourceDefn
  {
    public static final String TYPE_KEY = InlineInputSource.TYPE_KEY;
    public static final String DATA_PROPERTY = "data";

    @Override
    public String typeValue()
    {
      return TYPE_KEY;
    }

    @Override
    protected Class<? extends InputSource> inputSourceClass()
    {
      return InlineInputSource.class;
    }

    @Override
    protected List<ParameterDefn> adHocTableFnParameters()
    {
      return Collections.singletonList(
          new Parameter(DATA_PROPERTY, String.class, false)
      );
    }

    @Override
    public TableFunction partialTableFn(ResolvedExternalTable table)
    {
      return new PartialTableFunction(table, Collections.emptyList());
    }

    @Override
    public void validate(ResolvedExternalTable table)
    {
      // For inline, format is required to match the data
      if (table.formatMap() == null) {
        throw new IAE("An inline input source must provide a format.");
      }
      if (CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns())) {
        throw new IAE("An inline input source must provide one or more columns");
      }

      super.validate(table);
    }

    @Override
    protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
    {
      jsonMap.put(InputSource.TYPE_PROPERTY, InlineInputSource.TYPE_KEY);
      String data = CatalogUtils.getString(args, DATA_PROPERTY);

      // Would be nice, from a completeness perspective, for the inline data
      // source to allow zero rows of data. However, such is not the case.
      if (Strings.isNullOrEmpty(data)) {
        throw new IAE(
            "An inline table requires one or more rows of data in the '%s' property",
            DATA_PROPERTY
        );
      }
      jsonMap.put("data", data);
    }

    @Override
    protected ExternalTableSpec convertCompletedTable(
        final ResolvedExternalTable table,
        final Map<String, Object> args,
        final List<ColumnSpec> columns
    )
    {
      if (!args.isEmpty()) {
        throw new ISE("Cannot provide arguments for an inline table");
      }
      if (!columns.isEmpty()) {
        throw new IAE("Cannot provide columns for an inline table");
      }
      return convertTable(table);
    }

    @Override
    protected void auditTableProperties(Map<String, Object> jsonMap)
    {
      // Special handling of the data property which, in SQL, is a null-delimited
      // list of rows. The user will usually provide a trailing newline which should
      // not be interpreted as an empty data row. That is, if the data ends with
      // a newline, the inline input source will interpret that as a blank line, oddly.
      String data = CatalogUtils.getString(jsonMap, "data");
       if (data != null && data.endsWith("\n")) {
        jsonMap.put("data", data.trim());
      }
    }
  }

  /**
   * Definition of an HTTP input source source.
   * <p>
   * Provides a parameterized form where the user defines a value for the
   * {@code uriTemplate} table property in the table spec, then provides the partial URLs
   * in a table function to use for that one query. The final URIs are created by combining
   * the template and the arguments. Example:
   * <li>{@code uriTemplate} property): "http://example.com/data/kttm-{}.json"</li>
   * <li>{@code uris} function argument: "22-Nov-21, 22-Nov-22"</li>
   * </ul>
   * <p>
   * When the template is used, the format is optional: it can be provided either with
   * the table spec or at runtime, depending on what the user wants to accomplish. In the
   * above, where the ".json" is encoded in the template, it makes sense to include the format
   * with the spec. If the template was "http://example.com/data/{}", and the data comes in
   * multiple formats, it might make sense to specify the format in the query. In this case,
   * the table spec acts more like a connection.
   * <p>
   * If the template is not used, then the {@code uris} property must be provided in the
   * table spec, along with the corresponding format.
   * <p>
   * The above semantics make a bit more sense when we realize that the spec can also
   * provide a user name and password. When those are provided, then the input source must
   * name a single site: the one for which the credentials are valid. Given this, the only
   * table spec that makes sense is one where the URI is defined: either as a template or
   * explicitly.
   * <p>
   * When used as an ad-hoc function, the user specifies the uris and optional user name
   * and password: the template is not available (or useful) in the ad-hoc case.
   * <p>
   * Table function parameters are cleaned up relative to the input source field names to
   * make them a bit easier to use.
   */
  public static class HttpInputSourceDefn extends FormattedInputSourceDefn
  {
    public static final String TYPE_KEY = HttpInputSource.TYPE_KEY;

    // Catalog properties that map to fields in the HttpInputSource. See
    // that class for the meaning of these properties.

    public static final String URI_TEMPLATE_PROPERTY = "uriTemplate";

    public static final String URIS_PARAMETER = "uris";

    // Note, cannot be the simpler "user" since USER is a reserved word in SQL
    // and we don't want to require users to quote "user" each time it is used.
    public static final String USER_PARAMETER = "userName";
    public static final String PASSWORD_PARAMETER = "password";
    public static final String PASSWORD_ENV_VAR_PARAMETER = "passwordEnvVar";

    private static final List<ParameterDefn> URI_PARAMS = Arrays.asList(
        new Parameter(URIS_PARAMETER, String.class, false)
    );

    private static final List<ParameterDefn> USER_PWD_PARAMS = Arrays.asList(
        new Parameter(USER_PARAMETER, String.class, true),
        new Parameter(PASSWORD_PARAMETER, String.class, true),
        new Parameter(PASSWORD_ENV_VAR_PARAMETER, String.class, true)
    );

    // Field names in the HttpInputSource
    private static final String URIS_FIELD = "uris";
    private static final String PASSWORD_FIELD = "httpAuthenticationPassword";
    private static final String USERNAME_FIELD = "httpAuthenticationUsername";

    @Override
    public String typeValue()
    {
      return TYPE_KEY;
    }

    @Override
    protected Class<? extends InputSource> inputSourceClass()
    {
      return HttpInputSource.class;
    }

    @Override
    public void validate(ResolvedExternalTable table)
    {
      final Map<String, Object> sourceMap = table.sourceMap();
      final boolean hasUri = sourceMap.containsKey(URIS_FIELD);
      final String uriTemplate = table.resolvedTable().stringProperty(URI_TEMPLATE_PROPERTY);
      final boolean hasTemplate = uriTemplate != null;
      final boolean hasFormat = table.formatMap() != null;
      final boolean hasColumns = !CollectionUtils.isNullOrEmpty(table.resolvedTable().spec().columns());

      if (!hasUri && !hasTemplate) {
        throw new IAE(
            "External HTTP tables must provide either a URI or a %s property",
            URI_TEMPLATE_PROPERTY
        );
      }
      if (hasUri && hasTemplate) {
        throw new IAE(
            "External HTTP tables must provide only one of a URI or a %s property",
            URI_TEMPLATE_PROPERTY
        );
      }
      if (hasUri && !hasFormat) {
        throw new IAE(
            "An external HTTP tables with a URI must also provide the corresponding format"
        );
      }
      if (hasUri && !hasColumns) {
        throw new IAE(
            "An external HTTP tables with a URI must also provide the corresponding columns"
        );
      }
      if (hasTemplate) {

        // Verify the template
        templateMatcher(uriTemplate);

        // Patch in a dummy URI so that validation of the rest of the fields
        // will pass.
        try {
          sourceMap.put(
              URIS_FIELD,
              Collections.singletonList(new URI("https://bogus.com/file"))
          );
        }
        catch (Exception e) {
          throw new ISE(e, "URI parse failed");
        }
      }
      super.validate(table);
    }

    private Matcher templateMatcher(String uriTemplate)
    {
      Pattern p = Pattern.compile("\\{}");
      Matcher m = p.matcher(uriTemplate);
      if (!m.find()) {
        throw new IAE(
            "Value [%s] for property %s must include a '{}' placeholder",
            uriTemplate,
            URI_TEMPLATE_PROPERTY
        );
      }
      return m;
    }

    @Override
    protected List<ParameterDefn> adHocTableFnParameters()
    {
      return CatalogUtils.concatLists(URI_PARAMS, USER_PWD_PARAMS);
    }

    @Override
    protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
    {
      jsonMap.put(InputSource.TYPE_PROPERTY, HttpInputSource.TYPE_KEY);
      convertUriArgs(jsonMap, args);
      convertUserPasswordArgs(jsonMap, args);
    }

    @Override
    public TableFunction partialTableFn(ResolvedExternalTable table)
    {
      List<ParameterDefn> params = Collections.emptyList();

      // Does the table define URIs?
      Map<String, Object> sourceMap = table.sourceMap();
      if (!sourceMap.containsKey(URIS_FIELD)) {
        params = CatalogUtils.concatLists(params, URI_PARAMS);
      }

      // Does the table define a user or password?
      if (!sourceMap.containsKey(USERNAME_FIELD) && !sourceMap.containsKey(PASSWORD_FIELD))
      {
        params = CatalogUtils.concatLists(params, USER_PWD_PARAMS);
      }

      // Does the table define a format?
      if (table.formatMap() == null) {
        params = addFormatParameters(params);
      }
      return new PartialTableFunction(table, params);
    }

    @Override
    protected ExternalTableSpec convertCompletedTable(
        final ResolvedExternalTable table,
        final Map<String, Object> args,
        final List<ColumnSpec> columns
    )
    {
      final ObjectMapper jsonMapper = table.resolvedTable().jsonMapper();

      // Choose table or SQL-provided columns: table takes precedence.
      final List<ColumnSpec> completedCols;
      final List<ColumnSpec> tableCols = table.resolvedTable().spec().columns();
      if (CollectionUtils.isNullOrEmpty(tableCols)) {
        completedCols = columns;
      } else if (!columns.isEmpty()) {
          throw new IAE(
              "Catalog definition for the %s input source already contains column definitions",
              typeValue()
          );
      } else {
        completedCols = tableCols;
      }

      // Get URIs from table if defined, else from arguments.
      final Map<String, Object> sourceMap = new HashMap<>(table.sourceMap());
      final String uriTemplate = table.resolvedTable().stringProperty(URI_TEMPLATE_PROPERTY);
      if (uriTemplate != null) {
        convertUriTemplateArgs(sourceMap, uriTemplate, args);
      } else if (!sourceMap.containsKey(URIS_FIELD)) {
        convertUriArgs(sourceMap, args);
      }

      // Get user and password from the table if defined, else from arguments.
      if (!sourceMap.containsKey(USERNAME_FIELD) && !sourceMap.containsKey(PASSWORD_FIELD))
      {
        convertUserPasswordArgs(sourceMap, args);
      }

      // Get the format from the table, if defined, else from arguments.
      final InputFormat inputFormat;
      if (table.formatMap() == null) {
        inputFormat = convertArgsToFormat(args, completedCols, jsonMapper);
      } else {
        inputFormat = convertTableToFormat(table);
      }

      return new ExternalTableSpec(
          convertSource(sourceMap, jsonMapper),
          inputFormat,
          Columns.convertSignature(completedCols)
      );
    }

    private void convertUriTemplateArgs(Map<String, Object> jsonMap, String uriTemplate, Map<String, Object> args)
    {
      final Matcher m = templateMatcher(uriTemplate);
      final List<String> uris = getUriListArg(args).stream()
          .map(uri -> m.replaceFirst(uri))
          .collect(Collectors.toList());
      jsonMap.put(URIS_FIELD, convertUriList(uris));
    }

    /**
     * URIs in SQL is in the form of a string that contains a comma-delimited
     * set of URIs. Done since SQL doesn't support array scalars.
     */
    private void convertUriArgs(Map<String, Object> jsonMap, Map<String, Object> args)
    {
      jsonMap.put(URIS_FIELD, convertUriList(getUriListArg(args)));
    }

    private List<String> getUriListArg(Map<String, Object> args)
    {
      String urisString = CatalogUtils.getString(args, URIS_PARAMETER);
      if (Strings.isNullOrEmpty(urisString)) {
        throw new IAE("One or more values are required for parameter %s", URIS_PARAMETER);
      }
      return CatalogUtils.stringToList(urisString);
    }

    /**
     * Convert the user name and password. All are SQL strings. Passwords must be in
     * the form of a password provider, so do the needed conversion. HTTP provides
     * two kinds of passwords (plain test an reference to an env var), but at most
     * one can be provided.
     */
    private void convertUserPasswordArgs(Map<String, Object> jsonMap, Map<String, Object> args)
    {
      String user = CatalogUtils.getString(args, USER_PARAMETER);
      if (user != null) {
        jsonMap.put(USERNAME_FIELD, user);
      }
      String password = CatalogUtils.getString(args, PASSWORD_PARAMETER);
      String passwordEnvVar = CatalogUtils.getString(args, PASSWORD_ENV_VAR_PARAMETER);
      if (password != null && passwordEnvVar != null) {
        throw new ISE(
            "Specify only one of %s or %s",
            PASSWORD_PARAMETER,
            PASSWORD_ENV_VAR_PARAMETER
        );
      }
      if (password != null) {
        jsonMap.put(
            PASSWORD_FIELD,
            ImmutableMap.of("type", DefaultPasswordProvider.TYPE_KEY, "password", password)
        );
      } else if (passwordEnvVar != null) {
        jsonMap.put(
            PASSWORD_FIELD,
            ImmutableMap.of("type", EnvironmentVariablePasswordProvider.TYPE_KEY, "variable", passwordEnvVar)
        );
      }
    }

    /**
     * Convert a list of strings to a list of {@link URI} objects.
     */
    public static List<URI> convertUriList(List<String> list)
    {
      List<URI> uris = new ArrayList<>();
      for (String strValue : list) {
        try {
          uris.add(new URI(strValue));
        }
        catch (URISyntaxException e) {
          throw new IAE(StringUtils.format("Argument [%s] is not a valid URI", strValue));
        }
      }
      return uris;
    }
  }
}
