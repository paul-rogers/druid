/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.druid.catalog.specs;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import org.apache.druid.utils.CollectionUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Metadata definition of the metadata objects stored in the catalog. (Yes,
 * that means that this is meta-meta-data.) Objects consist of a map of
 * property values (and perhaps other items defined in subclasses.) Each
 * property is defined by a column metadata object. Objects allow extended
 * properties which have no definition: the meaning of such properties is
 * defined elsewhere.
 */
public class CatalogObjectDefn
{
  private final String name;
  private final String typeValue;
  private final Map<String, CatalogFieldDefn<?>> fields;

  public CatalogObjectDefn(
      final String name,
      final String typeValue,
      final List<CatalogFieldDefn<?>> fields
  )
  {
    this.name = name;
    this.typeValue = typeValue;
    this.fields = toFieldMap(fields);
  }

  protected static Map<String, CatalogFieldDefn<?>> toFieldMap(final List<CatalogFieldDefn<?>> fields)
  {
    ImmutableMap.Builder<String, CatalogFieldDefn<?>> builder = ImmutableMap.builder();
    if (fields != null) {
      for (CatalogFieldDefn<?> field : fields) {
        builder.put(field.name(), field);
      }
    }
    return builder.build();
  }

  public String name()
  {
    return name;
  }

  /**
   * The type value is the value of the {@code "type"} field written into the
   * object's Java or JSON representation. It is akin to the type used by
   * Jackson.
   */
  public String typeValue()
  {
    return typeValue;
  }

  public Map<String, CatalogFieldDefn<?>> fields()
  {
    return fields;
  }

  public CatalogFieldDefn<?> resolveField(String key)
  {
    return fields.get(key);
  }

  /**
   * Merge the properties for an object using a set of updates in a map. If the
   * update value is null, then remove the property in the revised set. If the
   * property is known, use the column definition to merge the values. Else, the
   * update replaces any existing value.
   * <p>
   * This method does not validate the properties, except as needed to do a
   * merge. A separate validation step is done on the final, merged object.
   */
  protected Map<String, Object> mergeProperties(
      final Map<String, Object> source,
      final Map<String, Object> update
  )
  {
    if (update == null) {
      return source;
    }
    if (source == null) {
      return update;
    }
    Map<String, Object> tags = new HashMap<>(source);
    for (Map.Entry<String, Object> entry : update.entrySet()) {
      if (entry.getValue() == null) {
        tags.remove(entry.getKey());
      } else {
        CatalogFieldDefn<?> field = resolveField(entry.getKey());
        Object value = entry.getValue();
        if (field != null) {
          value = field.merge(tags.get(entry.getKey()), entry.getValue());
        }
        tags.put(entry.getKey(), value);
      }
    }
    return tags;
  }

  /**
   * Validate the property values using the property definitions defined in
   * this class. The list may contain "custom" properties which are accepted
   * as-is.
   */
  public void validate(Map<String, Object> spec, ObjectMapper jsonMapper)
  {
    for (CatalogFieldDefn<?> field : fields.values()) {
      field.validate(spec.get(field.name()), jsonMapper);
    }
  }
}
