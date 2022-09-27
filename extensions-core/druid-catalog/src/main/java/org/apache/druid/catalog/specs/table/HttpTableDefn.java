package org.apache.druid.catalog.specs.table;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.CatalogUtils;
import org.apache.druid.catalog.specs.Parameterized;
import org.apache.druid.catalog.specs.PropertyDefn.StringListPropertyDefn;
import org.apache.druid.catalog.specs.PropertyDefn.StringPropertyDefn;
import org.apache.druid.catalog.specs.TableSpec;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.table.InputTableDefn.FormattedInputTableDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
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

public class HttpTableDefn extends FormattedInputTableDefn implements Parameterized
{
  public static final String HTTP_TABLE_TYPE = HttpInputSource.TYPE_KEY;
  public static final String URI_TEMPLATE_PROPERTY = "template";
  public static final String USER_PROPERTY = "user";
  public static final String PASSWORD_PROPERTY = "password";
  public static final String PASSWORD_ENV_VAR_PROPERTY = "passwordEnvVar";
  public static final String URIS_PROPERTY = "uris";
  public static final String URIS_PARAMETER = "uris";

  private final List<ParameterDefn> parameters;

  public HttpTableDefn()
  {
    super(
        "HTTP input table",
        HTTP_TABLE_TYPE,
        Arrays.asList(
            new StringListPropertyDefn(URIS_PROPERTY),
            new StringPropertyDefn(USER_PROPERTY),
            new StringPropertyDefn(PASSWORD_PROPERTY),
            new StringPropertyDefn(PASSWORD_ENV_VAR_PROPERTY),
            new StringPropertyDefn(URI_TEMPLATE_PROPERTY)
        ),
        Collections.singletonList(INPUT_COLUMN_DEFN),
        InputFormats.ALL_FORMATS
    );
    parameters = Collections.singletonList(
        new ParameterImpl(URIS_PARAMETER, String.class)
    );
  }

  @Override
  public List<ParameterDefn> parameters()
  {
    return parameters;
  }

  @Override
  public TableSpec mergeParameters(ResolvedTable table, Map<String, Object> values)
  {
    String urisValue = CatalogUtils.safeCast(
        values.get(URIS_PARAMETER),
        String.class,
        URIS_PARAMETER
    );
    List<String> uriValues = CatalogUtils.stringToList(urisValue);
    if (CollectionUtils.isNullOrEmpty(uriValues)) {
      throw new IAE("One or more values is required for parameter %s", URIS_PARAMETER);
    }
    String uriTemplate = table.stringProperty(URI_TEMPLATE_PROPERTY);
    if (Strings.isNullOrEmpty(uriTemplate)) {
      throw new IAE("Property %s must provide a URI template.", URI_TEMPLATE_PROPERTY);
    }
    Pattern p = Pattern.compile("\\{\\}");
    Matcher m = p.matcher(uriTemplate);
    if (!m.find()) {
      throw new IAE(
          "Value [%s] for property %s must include a '{}' placeholder.",
          uriTemplate,
          URI_TEMPLATE_PROPERTY
      );
    }
    List<String> uris = new ArrayList<>();
    for (String uri : uriValues) {
      uris.add(m.replaceFirst(uri));
    }

    Map<String, Object> revisedProps = new HashMap<>(table.properties());
    revisedProps.remove(URI_TEMPLATE_PROPERTY);
    revisedProps.put("uris", uris);
    return table.spec().withProperties(revisedProps);
  }

  @Override
  protected InputSource convertSource(ResolvedTable table)
  {
    Map<String, Object> jsonMap = new HashMap<>();
    jsonMap.put("type", HttpInputSource.TYPE_KEY);
    jsonMap.put("httpAuthenticationUsername", table.stringProperty(USER_PROPERTY));
    String password = table.stringProperty(PASSWORD_PROPERTY);
    String passwordEnvVar = table.stringProperty(PASSWORD_ENV_VAR_PROPERTY);
    if (password != null && passwordEnvVar != null) {
      throw new ISE(
          "Specify only one of %s or %s",
          PASSWORD_PROPERTY,
          PASSWORD_ENV_VAR_PROPERTY
      );
    }
    if (password != null) {
      jsonMap.put(
          "httpAuthenticationPassword",
          ImmutableMap.of("type", DefaultPasswordProvider.TYPE_KEY, "password", password)
      );
    } else if (passwordEnvVar != null) {
      jsonMap.put(
          "httpAuthenticationPassword",
          ImmutableMap.of("type", EnvironmentVariablePasswordProvider.TYPE_KEY, "variable", passwordEnvVar)
      );
    }
    jsonMap.put("uris", convertUriList(table.stringListProperty(URIS_PROPERTY)));
    return convertObject(table.jsonMapper(), jsonMap, HttpInputSource.class);
  }

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

  @Override
  public ExternalSpec applyParameters(ResolvedTable table, Map<String, Object> parameters)
  {
    TableSpec revised = mergeParameters(table, parameters);
    return convertToExtern(revised, table.jsonMapper());
  }
}
