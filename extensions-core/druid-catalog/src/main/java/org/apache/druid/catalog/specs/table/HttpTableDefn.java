package org.apache.druid.catalog.specs.table;

import com.google.common.base.Strings;
import org.apache.druid.catalog.specs.CatalogFieldDefn;
import org.apache.druid.catalog.specs.CatalogUtils;
import org.apache.druid.catalog.specs.FieldTypes.FieldClassDefn;
import org.apache.druid.catalog.specs.Parameterized;
import org.apache.druid.catalog.specs.TableSpec;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;
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

public class HttpTableDefn extends InputTableDefn implements Parameterized
{
  public static final String HTTP_TABLE_TYPE = HttpInputSource.TYPE_KEY;
  public static final String URI_TEMPLATE_PROPERTY = "template";
  public static final String URIS_PARAMETER = "uris";

  private final List<ParameterDefn> parameters;

  public HttpTableDefn()
  {
    super(
        "HTTP input table",
        HTTP_TABLE_TYPE,
        Arrays.asList(
            new CatalogFieldDefn<>(INPUT_SOURCE_PROPERTY, new FieldClassDefn<HttpInputSource>(HttpInputSource.class)),
            new CatalogFieldDefn.StringFieldDefn(URI_TEMPLATE_PROPERTY)
        ),
        Collections.singletonList(INPUT_COLUMN_DEFN)
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
      throw new ISE("One or more values is required for parameter %s", URIS_PARAMETER);
    }
    String uriTemplate = table.stringProperty(URI_TEMPLATE_PROPERTY);
    if (Strings.isNullOrEmpty(uriTemplate)) {
      throw new ISE("Property %s must provide a URI template.", URI_TEMPLATE_PROPERTY);
    }
    Pattern p = Pattern.compile("\\{\\}");
    Matcher m = p.matcher(uriTemplate);
    if (!m.matches()) {
      throw new ISE(
          "Value [%s] for property %s must include a '{}' placeholder.",
          uriTemplate,
          URI_TEMPLATE_PROPERTY
      );
    }
    List<String> uris = new ArrayList<>();
    for (String uri : uriValues) {
      String fullUri = m.replaceFirst(uri);
      try {
        new URI(fullUri);
      }
      catch (URISyntaxException e) {
        throw new IAE(
            "Argument [%s] is not a valid URI when used in template [%s] to give [%s]",
            uri,
            uriTemplate,
            fullUri
        );
      }
      uris.add(fullUri);
    }

    @SuppressWarnings("unchecked")
    Map<String, Object> inputSpec = CatalogUtils.safeCast(table.property(INPUT_SOURCE_PROPERTY), Map.class, INPUT_SOURCE_PROPERTY);
    if (inputSpec == null) {
      throw new IAE("Parameterized HTTP table requies an valid %", INPUT_SOURCE_PROPERTY);
    }
    Map<String, Object> revisedSourceSpec = new HashMap<>(inputSpec);
    revisedSourceSpec.put("uris", uris);
    Map<String, Object> revisedProps = new HashMap<>(table.properties());
    revisedProps.put(INPUT_SOURCE_PROPERTY, revisedSourceSpec);
    return table.spec().withProperties(revisedProps);
  }
}
