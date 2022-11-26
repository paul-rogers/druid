package org.apache.druid.catalog.model.table;

import org.apache.druid.catalog.model.TableDefnRegistry;

public interface InputSourceDefn
{
  void bind(TableDefnRegistry registry);
  String typeValue();
  void validate(ResolvedExternalTable resolvedExternalTable);
  TableFunction externFn();
  TableFunction tableFn(ResolvedExternalTable table);
  ExternalTableSpec convertTable(ResolvedExternalTable table);
}
