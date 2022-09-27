package org.apache.druid.catalog.specs.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.specs.CatalogFieldDefn;
import org.apache.druid.catalog.specs.ColumnDefn;
import org.apache.druid.catalog.specs.ColumnSpec;
import org.apache.druid.catalog.specs.Columns;
import org.apache.druid.catalog.specs.FieldTypes.FieldClassDefn;
import org.apache.druid.catalog.specs.TableDefn;
import org.apache.druid.catalog.specs.TableSpec;
import org.apache.druid.catalog.specs.table.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.data.input.InputFormat;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.java.util.common.ISE;

import java.util.Collections;
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
  public static final String INPUT_FORMAT_PROPERTY = "format";
  public static final String INPUT_SOURCE_PROPERTY = "source";
  public static final String INPUT_COLUMN_TYPE = "input";

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

  protected static final CatalogFieldDefn<?>[] inputTableFields = {
      new CatalogFieldDefn<>(INPUT_FORMAT_PROPERTY, new FieldClassDefn<InputFormat>(InputFormat.class))
  };

  public InputTableDefn(
      final String name,
      final String typeValue,
      final List<CatalogFieldDefn<?>> fields,
      final List<ColumnDefn> columnDefns
  )
  {
    super(name, typeValue, extendFields(inputTableFields, fields), columnDefns);
  }

  public abstract TableSpec mergeParameters(ResolvedTable spec, Map<String, Object> values);

  public ExternalSpec convertToExtern(TableSpec spec, ObjectMapper jsonMapper)
  {
    ResolvedTable table = new ResolvedTable(this, spec, jsonMapper);
    return new ExternalSpec(
        convertSource(table),
        InputFormats.INPUT_FORMAT_CONVERTER.convert(table),
        Columns.convertSignature(spec)
    );
  }

  protected InputSource convertSource(ResolvedTable table)
  {
    try {
      return table.jsonMapper().convertValue(
          table.properties().get(INPUT_SOURCE_PROPERTY),
          InputSource.class
      );
    }
    catch (Exception e)
    {
      throw new ISE(
          e,
          "Failed to deserialize an %s table to an instance of %s",
          name(),
          InputSource.class.getSimpleName()
      );
    }
  }
}
