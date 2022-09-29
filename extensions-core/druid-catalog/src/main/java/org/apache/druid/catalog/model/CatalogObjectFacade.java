package org.apache.druid.catalog.model;

import java.util.List;
import java.util.Map;

public abstract class CatalogObjectFacade
{
  public abstract Map<String, Object> properties();

  public Object property(String key)
  {
    return properties().get(key);
  }

  public boolean hasProperty(String key)
  {
    return properties().containsKey(key);
  }

  public boolean booleanProperty(String key)
  {
    return (Boolean) property(key);
  }

  public String stringProperty(String key)
  {
    return (String) property(key);
  }

  public Integer intProperty(String key)
  {
    return (Integer) property(key);
  }

  @SuppressWarnings("unchecked")
  public List<String> stringListProperty(String key)
  {
    return (List<String>) property(key);
  }
}
