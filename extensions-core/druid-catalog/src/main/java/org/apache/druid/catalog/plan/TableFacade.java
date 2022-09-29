package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.model.CatalogObjectFacade;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.ResolvedTable;
import org.apache.druid.catalog.model.TableSpec;

import java.util.List;
import java.util.Map;

public class TableFacade extends CatalogObjectFacade
{
  protected final ResolvedTable resolved;

  public TableFacade(ResolvedTable resolved)
  {
    this.resolved = resolved;
  }

  public TableSpec spec()
  {
    return resolved.spec();
  }

  @Override
  public Map<String, Object> properties()
  {
    return spec().properties();
  }

  public List<ColumnSpec> columns()
  {
    return spec().columns();
  }
}
