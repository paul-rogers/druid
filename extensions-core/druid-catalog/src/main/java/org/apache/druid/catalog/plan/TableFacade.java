package org.apache.druid.catalog.plan;

import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.CatalogObjectFacade;
import org.apache.druid.catalog.specs.TableSpec;

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
}
