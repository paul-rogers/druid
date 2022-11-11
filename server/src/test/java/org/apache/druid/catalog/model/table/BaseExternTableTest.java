package org.apache.druid.catalog.model.table;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.catalog.model.ModelProperties.PropertyDefn;

import java.util.List;

public class BaseExternTableTest
{
  protected final ObjectMapper mapper = new ObjectMapper();

  protected PropertyDefn<?> findProperty(List<PropertyDefn<?>> props, String name)
  {
    for (PropertyDefn<?> prop : props) {
      if (prop.name().equals(name)) {
        return prop;
      }
    }
    return null;
  }

}
