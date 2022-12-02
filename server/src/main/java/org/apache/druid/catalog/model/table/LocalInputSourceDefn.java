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

package org.apache.druid.catalog.model.table;

import com.google.common.base.Strings;
import org.apache.druid.catalog.model.CatalogUtils;
import org.apache.druid.catalog.model.ColumnSpec;
import org.apache.druid.catalog.model.table.BaseFunctionDefn.Parameter;
import org.apache.druid.catalog.model.table.TableFunction.ParameterDefn;
import org.apache.druid.data.input.InputSource;
import org.apache.druid.data.input.impl.LocalInputSource;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.utils.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Definition for a {@link LocalInputSource}.
 */
public class LocalInputSourceDefn extends FormattedInputSourceDefn
{
  public static final String TYPE_KEY = LocalInputSource.TYPE_KEY;

  /**
   * Base directory for file or filter operations. If not provided,
   * then the servers current working directory is assumed, which is
   * typically valid only for sample data.
   */
  public static final String BASE_DIR_PARAMETER = "baseDir";

  // Note name "fileFilter", not "filter". These properties mix in with
  // others and "filter" is a bit too generic in that context.
  public static final String FILTER_PARAMETER = "fileFilter";
  public static final String FILES_PARAMETER = "files";

  private static final String BASE_DIR_FIELD = "baseDir";
  private static final String FILES_FIELD = "files";
  private static final String FILTER_FIELD = "filter";

  private final ParameterDefn FILTER_PARAM_DEFN = new Parameter(FILTER_PARAMETER, String.class, true);
  private final ParameterDefn FILES_PARAM_DEFN = new Parameter(FILES_PARAMETER, String.class, true);

  @Override
  public String typeValue()
  {
    return LocalInputSource.TYPE_KEY;
  }

  @Override
  protected Class<? extends InputSource> inputSourceClass()
  {
    return LocalInputSource.class;
  }

  @Override
  public void validate(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.sourceMap());
    final boolean hasBaseDir = sourceMap.containsKey(BASE_DIR_FIELD);
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));

    if (!hasBaseDir && !hasFiles) {
      throw new IAE(
          "A local input source requires one property of %s or %s",
          BASE_DIR_FIELD,
          FILES_FIELD
      );
    }
    if (!hasBaseDir && hasFilter) {
      throw new IAE(
          "If a local input source sets property %s, it must also set property %s",
          FILTER_FIELD,
          BASE_DIR_FIELD
      );
    }
    super.validate(table);
  }

  @Override
  protected List<ParameterDefn> adHocTableFnParameters()
  {
    return Arrays.asList(
        new Parameter(BASE_DIR_PARAMETER, String.class, true),
        FILTER_PARAM_DEFN,
        FILES_PARAM_DEFN
    );
  }

  @Override
  protected void convertArgsToSourceMap(Map<String, Object> jsonMap, Map<String, Object> args)
  {
    jsonMap.put(InputSource.TYPE_PROPERTY, LocalInputSource.TYPE_KEY);

    final String baseDirParam = CatalogUtils.getString(args, BASE_DIR_PARAMETER);
    final String filesParam = CatalogUtils.getString(args, FILES_PARAMETER);
    final String filterParam = CatalogUtils.getString(args, FILTER_PARAMETER);
    if (Strings.isNullOrEmpty(baseDirParam) && Strings.isNullOrEmpty(filesParam)) {
      throw new IAE(
          "A local input source requires one parameter of %s or %s",
          BASE_DIR_PARAMETER,
          FILES_PARAMETER
      );
    }
    if (Strings.isNullOrEmpty(baseDirParam) && !Strings.isNullOrEmpty(filterParam)) {
      throw new IAE(
          "If a local input source sets property %s, it must also set property %s",
          FILTER_PARAMETER,
          BASE_DIR_PARAMETER
      );
    }
    if (baseDirParam != null) {
      jsonMap.put(BASE_DIR_FIELD, baseDirParam);
    }
    if (filesParam != null) {
      jsonMap.put(FILES_FIELD, CatalogUtils.stringToList(filesParam));
    }
    if (filterParam != null) {
      jsonMap.put(FILTER_FIELD, filterParam);
    }
  }

  @Override
  public TableFunction partialTableFn(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.sourceMap());
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
    List<ParameterDefn> params = new ArrayList<>();
    if (!hasFiles && !hasFilter) {
      params.add(FILES_PARAM_DEFN);
      params.add(FILTER_PARAM_DEFN);
    }

    // Does the table define a format?
    if (table.formatMap() == null) {
      params = addFormatParameters(params);
    }
    return new PartialTableFunction(table, params);
  }

  @Override
  protected ExternalTableSpec convertCompletedTable(
      final ResolvedExternalTable table,
      final Map<String, Object> args,
      final List<ColumnSpec> columns
  )
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.sourceMap());
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
    final String filesParam = CatalogUtils.getString(args, FILES_PARAMETER);
    final String filterParam = CatalogUtils.getString(args, FILTER_PARAMETER);
    if (!hasFiles && !hasFilter && Strings.isNullOrEmpty(filesParam) && Strings.isNullOrEmpty(filterParam)) {
      throw new IAE(
          "For a local input source, set either %s or %s",
          FILES_PARAMETER,
          FILTER_PARAMETER
      );
    }
    if (filesParam != null) {
      sourceMap.put(FILES_FIELD, CatalogUtils.stringToList(filesParam));
    }
    if (filterParam != null) {
      sourceMap.put(FILTER_FIELD, filterParam);
    }
    return convertPartialFormattedTable(table, args, columns, sourceMap);
  }

  @Override
  protected void auditInputSource(Map<String, Object> jsonMap)
  {
    // Note the odd semantics of this class.
    // If we give a base directory, and explicitly state files, we must
    // also provide a file filter which presumably matches the very files
    // we list. Take pity on the user and provide a filter in this case.
    String filter = CatalogUtils.getString(jsonMap, FILTER_FIELD);
    if (filter != null) {
      return;
    }
    String baseDir = CatalogUtils.getString(jsonMap, BASE_DIR_FIELD);
    if (baseDir != null) {
      jsonMap.put(FILTER_FIELD, "*");
    }
  }

  @Override
  public ExternalTableSpec convertTable(ResolvedExternalTable table)
  {
    final Map<String, Object> sourceMap = new HashMap<>(table.sourceMap());
    final boolean hasFiles = !CollectionUtils.isNullOrEmpty(CatalogUtils.safeGet(sourceMap, FILES_FIELD, List.class));
    final boolean hasFilter = !Strings.isNullOrEmpty(CatalogUtils.getString(sourceMap, FILTER_FIELD));
    if (!hasFiles && !hasFilter) {
      throw new IAE(
          "Use a table function to set either %s or %s",
          FILES_PARAMETER,
          FILTER_PARAMETER
      );
    }
    return super.convertTable(table);
  }
}
