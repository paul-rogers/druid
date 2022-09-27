package org.apache.druid.catalog.specs.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.catalog.specs.ColumnDefn;
import org.apache.druid.catalog.specs.ColumnSpec;
import org.apache.druid.catalog.specs.Columns;
import org.apache.druid.catalog.specs.PropertyDefn;
import org.apache.druid.catalog.specs.TableDefn;
import org.apache.druid.catalog.specs.TableSpec;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.table.InputFormats.InputFormatDefn;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.HttpInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.ISE;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 */
public abstract class InputTableDefn extends TableDefn
{
  public static final String INPUT_COLUMN_TYPE = "input";

  public abstract static class FormattedInputTableDefn extends InputTableDefn
  {
    public static final String INPUT_FORMAT_PROPERTY = "format";

    private Map<String, InputFormatDefn> formats;

    public FormattedInputTableDefn(
        final String name,
        final String typeValue,
        final List<PropertyDefn> fields,
        final List<ColumnDefn> columnDefns,
        final List<InputFormatDefn> formats
    )
    {
      super(
          name,
          typeValue,
          addFormatProperties(fields, formats),
          columnDefns
      );
      ImmutableMap.Builder<String, InputFormatDefn> builder = ImmutableMap.builder();
      for (InputFormatDefn format : formats) {
        builder.put(format.typeTag(), format);
      }
      this.formats = builder.build();
    }

    private static List<PropertyDefn> addFormatProperties(
        final List<PropertyDefn> properties,
        final List<InputFormatDefn> formats
    )
    {
      Map<String, PropertyDefn> formatProps = new HashMap<>();
      for (InputFormatDefn format : formats) {
        for (PropertyDefn prop : format.properties()) {
          PropertyDefn existing = formatProps.putIfAbsent(prop.name(), prop);
          if (existing != null && existing.getClass() != prop.getClass()) {
            throw new ISE(
                "Format %s, property %s of class %s conflicts with another format property of class %s",
                format.name(),
                prop.name(),
                prop.getClass().getSimpleName(),
                existing.getClass().getSimpleName()
            );
          }
        }
      }
      List<PropertyDefn> props = new ArrayList<>();
      if (properties != null) {
        props.addAll(properties);
      }
      props.addAll(formatProps.values());
      return props;
    }

    @Override
    protected InputFormat convertFormat(ResolvedTable table)
    {
      return formatDefn(table).convert(table);
    }

    protected InputFormatDefn formatDefn(ResolvedTable table)
    {
      String formatTag = table.stringProperty(INPUT_FORMAT_PROPERTY);
      if (formatTag == null) {
        throw new IAE("%s property must be set", INPUT_FORMAT_PROPERTY);
      }
      InputFormatDefn formatDefn = formats.get(formatTag);
      if (formatDefn == null) {
        throw new IAE(
            "Format type [%s] for property %s is not valid",
            formatTag,
            INPUT_FORMAT_PROPERTY
        );
      }
      return formatDefn;
    }

    @Override
    public void validate(ResolvedTable table)
    {
      super.validate(table);
      formatDefn(table).validate(table);
    }
  }

  /**
   * Definition of a column in a detail (non-rollup) datasource.
   */
  public static class InputColumnDefn extends ColumnDefn
  {
    public InputColumnDefn()
    {
      super(
          "Column",
          INPUT_COLUMN_TYPE,
          null
      );
    }

    @Override
    public void validate(ColumnSpec spec, ObjectMapper jsonMapper)
    {
      super.validate(spec, jsonMapper);
      validateScalarColumn(spec);
    }
  }

  protected static final InputColumnDefn INPUT_COLUMN_DEFN = new InputColumnDefn();

  public InputTableDefn(
      final String name,
      final String typeValue,
      final List<PropertyDefn> fields,
      final List<ColumnDefn> columnDefns
  )
  {
    super(name, typeValue, fields, columnDefns);
  }

  public abstract TableSpec mergeParameters(ResolvedTable spec, Map<String, Object> values);

  public ExternalSpec convertToExtern(TableSpec spec, ObjectMapper jsonMapper)
  {
    ResolvedTable table = new ResolvedTable(this, spec, jsonMapper);
    return new ExternalSpec(
        convertSource(table),
        convertFormat(table),
        Columns.convertSignature(spec)
    );
  }

  protected InputFormat convertFormat(ResolvedTable table)
  {
    return null;
  }

  protected abstract InputSource convertSource(ResolvedTable table);

  protected InputSource convertObject(
      final ObjectMapper jsonMapper,
      final Map<String, Object> jsonMap,
      final Class<? extends InputSource> targetClass
  )
  {
    try {
      return jsonMapper.convertValue(jsonMap, targetClass);
    }
    catch (Exception e) {
      throw new IAE(e, "Invalid table specification");
    }
  }
}
