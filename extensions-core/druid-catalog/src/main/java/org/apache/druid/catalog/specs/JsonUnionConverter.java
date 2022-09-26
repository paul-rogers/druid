package org.apache.druid.catalog.specs;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.druid.catalog.specs.CatalogTableRegistry.ResolvedTable;
import org.apache.druid.catalog.specs.JsonObjectConverter.JsonSubclassConverter;
import org.apache.druid.java.util.common.IAE;

import java.util.List;

public class JsonUnionConverter<T>
{
  private final String typeName;
  private final String typePropertyName;
  private final String jsonTypeFieldName;
  private final List<JsonSubclassConverter<? extends T>> subtypes;

  public JsonUnionConverter(
      final String typeName,
      final String typePropertyName,
      final String jsonTypeFieldName,
      final List<JsonSubclassConverter<? extends T>> subtypes
  )
  {
    Preconditions.checkArgument(!Strings.isNullOrEmpty(typePropertyName));
    Preconditions.checkArgument(!Strings.isNullOrEmpty(jsonTypeFieldName));
    this.typeName = typeName;
    this.typePropertyName = typePropertyName;
    this.jsonTypeFieldName = jsonTypeFieldName;
    this.subtypes = subtypes;
  }

  public T convert(ResolvedTable table)
  {
    String typeValue = table.stringProperty(typePropertyName);
    if (typeValue == null) {
      throw new IAE(
          "The type property %s is required for type %s",
          typePropertyName,
          typeName
      );
    }
    for (JsonSubclassConverter<? extends T> converter : subtypes) {
      if (converter.typePropertyValue().equals(typeValue)) {
        return converter.convert(table, jsonTypeFieldName);
      }
    }
    throw new IAE(
        "The type value %s is not defined for type %s",
        typePropertyName,
        typeName
    );
  }
}
