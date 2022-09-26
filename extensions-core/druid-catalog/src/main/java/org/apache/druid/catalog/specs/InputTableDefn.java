package org.apache.druid.catalog.specs;

import java.util.Map;

/**
 * Definition of an external input source, primarily for ingestion.
 * The components are derived from those for Druid ingestion: an
 * input source, a format and a set of columns. Also provides
 * properties, as do all table definitions.
 */
public class InputTableDefn extends TableDefn
{
  public InputTableDefn(String name, String typeValue, Map<String, CatalogFieldDefn<?>> fields,
      Map<String, ColumnDefn> columnDefns)
  {
    super(name, typeValue, fields, columnDefns);
  }

}
