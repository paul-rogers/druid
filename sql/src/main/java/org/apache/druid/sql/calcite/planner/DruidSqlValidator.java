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

package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.prepare.BaseDruidSqlValidator;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelRecordType;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.BasicSqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.IdentifierNamespace;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.calcite.sql.validate.SqlValidatorNamespace;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.sql.validate.SqlValidatorTable;
import org.apache.calcite.util.Pair;
import org.apache.druid.catalog.model.Columns;
import org.apache.druid.catalog.model.facade.DatasourceFacade;
import org.apache.druid.catalog.model.facade.DatasourceFacade.ColumnFacade;
import org.apache.druid.catalog.model.table.ClusterKeySpec;
import org.apache.druid.common.utils.IdUtils;
import org.apache.druid.java.util.common.IAE;
import org.apache.druid.java.util.common.StringUtils;
import org.apache.druid.java.util.common.granularity.Granularity;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;
import org.apache.druid.sql.calcite.parser.DruidSqlInsert;
import org.apache.druid.sql.calcite.table.DatasourceTable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Druid extended SQL validator.
 */
class DruidSqlValidator extends BaseDruidSqlValidator
{
  private static final Pattern UNNAMED_COLUMN_PATTERN = Pattern.compile("^EXPR\\$\\d+$", Pattern.CASE_INSENSITIVE);
  @VisibleForTesting
  public static final String UNNAMED_INGESTION_COLUMN_ERROR =
      "Expressions must provide an alias to specify the target column: func(X) AS myColumn";

  public interface ValidatorContext
  {
    Map<String, Object> queryContextMap();
    CatalogResolver catalog();
    String druidSchemaName();
    ObjectMapper jsonMapper();
  }

  private final ValidatorContext validatorContext;

  protected DruidSqlValidator(
      final SqlOperatorTable opTab,
      final CalciteCatalogReader catalogReader,
      final JavaTypeFactory typeFactory,
      final SqlConformance conformance,
      final ValidatorContext validatorContext
  )
  {
    super(opTab, catalogReader, typeFactory, conformance);
    this.validatorContext = validatorContext;
  }

  /**
   * Druid-specific validation for an INSERT statement. In Druid, the columns are
   * matched by name. A datasource, by default, allows the insertion of arbitrary columns,
   * but the catalog may enforce a strict schema (all columns must exist). Destination
   * types are set by the catalog, where available, else by the query.
   * <p>
   * The Druid {@code INSERT} statement is non-standard in a variety of ways:
   * <ul>
   * <li>Allows the target table to not yet exist. Instead, {@code INSERT}
   * creates it.</li>
   * <li>Does not allow specifying the list of columns:
   * {@code INSERT INTO dst (a, b, c) ...}</li>
   * <li>When given without target columns (the only form allowed), columns are
   * not matched by schema position as in standard SQL, but rather by name.</li>
   * <li>There is no requirement that the target columns already exist. In fact,
   * even if the target column exists, any existing type is ignored if not specified
   * in the catalog.</li>
   * <li>The source can only be a {@code SELECT} statement, not {@code VALUES}.</li>
   * <li>Types thus propagate upwards from the {@code SELECT} to the the target
   * table. Standard SQL says that types propagate down from the target table to the
   * source.</li>
   * <li>The __time column is special in multiple ways.</li>
   * <li>Includes the {@code CLUSTERED BY} and {@code PARTITIONED BY} clauses.</li>
   * </ul>
   * The result is that the validation for the Druid {@code INSERT} is wildly customized
   * relative to standard SQL.
   */
  // TODO: Ensure the source and target are not the same
  @Override
  public void validateInsert(SqlInsert insert)
  {
    DruidSqlIngest ingestNode = (DruidSqlIngest) insert;
    if (insert.isUpsert()) {
      throw new IAE("UPSERT is not supported.");
    }

    // SQL-style INSERT INTO dst (a, b, c) is not (yet) supported.
    String operationName = insert.getOperator().getName();
    if (insert.getTargetColumnList() != null) {
      throw new IAE("%s with a target column list is not supported.", operationName);
    }

    // The target namespace is both the target table ID and the row type for that table.
    final SqlValidatorNamespace targetNamespace = getNamespace(insert);
    IdentifierNamespace insertNs = (IdentifierNamespace) targetNamespace;

    // The target is a new or existing datasource.
    DatasourceTable table = validateInsertTarget(targetNamespace, insertNs, operationName);

    // An existing datasource may have metadata.
    DatasourceFacade tableMetadata = table == null ? null : table.effectiveMetadata().catalogMetadata();

    // Validate segment granularity, which depends on nothing else.
    validateSegmentGranularity(operationName, ingestNode, tableMetadata);

    // Validate the source statement. The result of this is needed to validate clustering.
    SqlNode source = insert.getSource();
    SqlValidatorScope selectScope = validateInsertSource(source, operationName);

    SqlValidatorNamespace sourceNamespace = namespaces.get(source);
    RelRecordType sourceType = (RelRecordType) sourceNamespace.getRowType();
    List<String> fieldNames = sourceType.getFieldNames();

    // Validate the __time column
    int timeColumnIndex = fieldNames.indexOf(Columns.TIME_COLUMN);
    if (timeColumnIndex != -1) {
      validateTimeColumn(sourceType, timeColumnIndex);
    }

    // Validate clustering against the SELECT row type
    validateClustering(sourceType, timeColumnIndex, ingestNode, tableMetadata, selectScope);

    // Determine the output (target) schema.
    RelDataType targetType = validateTargetType(insert, sourceType, tableMetadata);

    // Set the type for the INSERT/REPLACE node
    setValidatedNodeType(insert, targetType);
  }

  private SqlValidatorScope validateInsertSource(
      final SqlNode source,
      final String operationName
  )
  {
    // The source SELECT cannot include an ORDER BY clause. Ordering is given
    // by the CLUSTERED BY clause, if any.
    // Check that an ORDER BY clause is not provided by the underlying query
    if (source instanceof SqlOrderBy) {
      SqlOrderBy sqlOrderBy = (SqlOrderBy) source;
      SqlNodeList orderByList = sqlOrderBy.orderList;
      if (!(orderByList == null || orderByList.equals(SqlNodeList.EMPTY))) {
        throw new IAE(
            "Cannot have ORDER BY on %s %s statement, use CLUSTERED BY instead.",
            "INSERT".equals(operationName) ? "an" : "a",
            operationName
        );
      }
    }
    // Validate the query: but we only know how to do SELECT.
    // As we do, get the scope for the query.
    if (!source.isA(SqlKind.QUERY)) {
      throw new IAE("Cannot execute %s.", source.getKind());
    }
    if (source instanceof SqlSelect) {
      final SqlSelect sqlSelect = (SqlSelect) source;
      validateSelect(sqlSelect, unknownType);
      return getSelectScope(sqlSelect);
    } else {
      SqlValidatorScope selectScope = scopes.get(source);
      validateQuery(source, selectScope, unknownType);
      return selectScope;
    }
  }

  /**
   * Validate the target table. Druid {@code INSERT/REPLACE} can create a new datasource,
   * or insert into an existing one. If the target exists, it must be a datasource. If it
   * does not exist, the target must be in the datasource schema, normally "druid".
   */
  private DatasourceTable validateInsertTarget(
      final SqlValidatorNamespace targetNamespace,
      final IdentifierNamespace insertNs,
      final String operationName
  )
  {
    // Get the target table ID
    SqlIdentifier destId = insertNs.getId();
    if (destId.names.isEmpty()) {
      // I don't think this can happen, but include a branch for it just in case.
      throw new IAE("%s requires a target table.", operationName);
    }

    // Druid does not support 3+ part names.
    int n = destId.names.size();
    if (n > 2) {
      throw new IAE("Table name is undefined: %s", destId.toString());
    }
    String tableName = destId.names.get(n - 1);

    // If this is a 2-part name, the first part must be the datasource schema.
    if (n == 2 && !validatorContext.druidSchemaName().equals(destId.names.get(0))) {
      throw new IAE("Cannot %s into %s because it is not a Druid datasource.",
          operationName,
          destId
      );
    }
    try {
      // Try to resolve the table. Will fail if this is an INSERT into a new table.
      validateNamespace(targetNamespace, unknownType);
      SqlValidatorTable target = insertNs.resolve().getTable();
      try {
        return target.unwrap(DatasourceTable.class);
      }
      catch (Exception e) {
        throw new IAE("Cannot %s into %s: it is not a datasource", operationName, destId);
      }
    }
    catch (CalciteContextException e)
    {
      // Something failed. Let's make sure it was the table lookup.
      // The check is kind of a hack, but its the best we can do given that Calcite
      // didn't expect this non-SQL use case.
      if (e.getCause() instanceof SqlValidatorException && e.getMessage().contains(StringUtils.format("Object '%s' not found", tableName))) {
        // New table. Validate the shape of the name.
        IdUtils.validateId(operationName + " dataSource", tableName);
        return null;
      }
      throw e;
    }
  }

  private void validateSegmentGranularity(
      final String operationName,
      final DruidSqlIngest ingestNode,
      final DatasourceFacade tableMetadata
  )
  {
    final Granularity definedGranularity = tableMetadata == null ? null : tableMetadata.segmentGranularity();
    final Granularity ingestionGranularity = ingestNode.getPartitionedBy();
    final Granularity finalGranularity;
    if (definedGranularity == null && ingestionGranularity == null) {
      // Neither have a value: error
      throw new IAE(
          "%s statements must specify a PARTITIONED BY clause explicitly",
          operationName
      );
    } else if (ingestionGranularity == null) {
      // The query has no granularity: just apply the catalog granularity.
      finalGranularity = definedGranularity;
    } else if (definedGranularity == null) {
      // The catalog has no granularity: apply the query value
      finalGranularity = ingestionGranularity;
    } else if (definedGranularity.equals(ingestionGranularity)) {
      // Both have a setting. They have to be the same.
      finalGranularity = definedGranularity;
    } else {
      throw new IAE(
          "PARTITIONED BY mismatch. Catalog: [%s], query: [%s]",
          definedGranularity,
          ingestionGranularity
      );
    }

    // Note: though this is the validator, we cheat a bit and write the target
    // granularity into the query context. Perhaps this step should be done
    // during conversion, however, we've just worked out the granularity, so we
    // do it here instead.
    try {
      validatorContext.queryContextMap().put(
          DruidSqlInsert.SQL_INSERT_SEGMENT_GRANULARITY,
          validatorContext.jsonMapper().writeValueAsString(finalGranularity)
      );
    }
    catch (JsonProcessingException e) {
      throw new IAE("Invalid PARTITIONED BY granularity");
    }
  }

  private void validateTimeColumn(RelRecordType sourceType, int timeColumnIndex)
  {
    RelDataTypeField timeCol = sourceType.getFieldList().get(timeColumnIndex);
    RelDataType timeColType = timeCol.getType();
    if (!(timeColType instanceof BasicSqlType)) {
      throw new IAE("Invalid % column type %s: must be BIGINT or TIMESTAMP", timeCol.getName(), timeColType.toString());
    }
    BasicSqlType timeColSqlType = (BasicSqlType) timeColType;
    SqlTypeName timeColSqlTypeName = timeColSqlType.getSqlTypeName();
    if (timeColSqlTypeName != SqlTypeName.BIGINT && timeColSqlTypeName != SqlTypeName.TIMESTAMP) {
      throw new IAE("Invalid % column type %s: must be BIGINT or TIMESTAMP", timeCol.getName(), timeColType.toString());
    }
  }

  /**
   * Verify clustering which can come from the query, the catalog or both. If both,
   * the two must match. In either case, the cluster keys must be present in the SELECT
   * clause. The {@code __time} column cannot be included.
   */
  private void validateClustering(
      final RelRecordType sourceType,
      final int timeColumnIndex,
      final DruidSqlIngest ingestNode,
      final DatasourceFacade tableMetadata,
      final SqlValidatorScope selectScope
  )
  {
    List<ClusterKeySpec> keyCols = tableMetadata == null ? null : tableMetadata.clusterKeys();
    final SqlNodeList clusteredBy = ingestNode.getClusteredBy();

    // Validate both the catalog and query definitions if present. This ensures
    // that things are sane if we later check that the two are identical.
    if (clusteredBy != null) {
      validateClusteredBy(sourceType, timeColumnIndex, selectScope, clusteredBy);
    }
    if (keyCols != null) {
      // Catalog defines the key columns. Verify that they are present in the query.
      verifyCatalogClusterKeys(sourceType, timeColumnIndex, keyCols);
    }
    if (clusteredBy != null && keyCols != null) {
      // Both the query and catalog have keys.
      verifyQueryClusterByMatchesCatalog(sourceType, keyCols, clusteredBy);
    }
  }

  /**
   * Verify that each of the catalog keys matches a column in the SELECT clause. Also checks
   * for the {@code __time} column and for duplicates, though these checks should have been
   * done when writing the spec into the catalog.
   */
  private void verifyCatalogClusterKeys(
      final RelRecordType sourceType,
      final int timeColumnIndex,
      final List<ClusterKeySpec> keyCols
  )
  {
    // Keep track of fields which have been referenced.
    final List<String> fieldNames = sourceType.getFieldNames();
    final boolean[] refs = new boolean[fieldNames.size()];
    for (ClusterKeySpec keyCol : keyCols) {
      final String keyName = keyCol.expr();
      // Slow linear search. We assume that there are not many cluster keys.
      final int index = fieldNames.indexOf(keyName);
      if (index == -1) {
        throw new IAE("Cluster column '%s' defined in the catalog must be present in the query", keyName);
      }

      // Can't cluster by __time
      if (index == timeColumnIndex) {
        throw new IAE("Do not include %s in the cluster spec: it is managed by PARTITIONED BY", Columns.TIME_COLUMN);
      }

      // No duplicate references.
      if (refs[index]) {
        throw new IAE("Duplicate cluster key: '%s'", keyName);
      }
      refs[index] = true;
    }
  }

  /**
   * Validate the CLUSTERED BY list. Members can be any of the following:
   * <p>
   * {@code CLUSTERED BY [<ordinal> | <id> | <expr>] DESC?}
   * <p>
   * Ensure that each id exists. Ensure each column is included only once.
   * For an expression, just ensure it is valid; we don't check for duplicates.
   */
  private void validateClusteredBy(
      final RelRecordType sourceType,
      final int timeColumnIndex,
      final SqlValidatorScope selectScope,
      final SqlNodeList clusteredBy
  )
  {
    // Keep track of fields which have been referenced.
    final List<String> fieldNames = sourceType.getFieldNames();
    final int fieldCount = fieldNames.size();
    final boolean[] refs = new boolean[fieldCount];

    // Process cluster keys
    for (SqlNode clusterKey : clusteredBy) {

      // Check if the key is compound: only occurs for DESC. The ASC
      // case is abstracted away by the parser.
      if (clusterKey instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) clusterKey;
        if (basicCall.getOperator() == SqlStdOperatorTable.DESC) {
          // Cluster key is compound: CLUSTERED BY foo DESC
          // We check only the first element
          clusterKey = ((SqlBasicCall) clusterKey).getOperandList().get(0);
        }
      }

      // We now have the actual key. Handle the three cases.
      if (clusterKey instanceof SqlNumericLiteral) {
        // Key is an ordinal: CLUSTERED BY 2
        // Ordinals are 1-based.
        int ord = ((SqlNumericLiteral) clusterKey).intValue(true);
        int index = ord - 1;

        // The ordinal has to be in range.
        if (index < 0 || fieldCount <= index) {
          throw new IAE("CLUSTERED BY ordinal %d is not valid", ord);
        }

        // Can't cluster by __time
        if (index == timeColumnIndex) {
          throw new IAE("Do not include %s in the CLUSTERED BY clause: it is managed by PARTITIONED BY", Columns.TIME_COLUMN);
        }
        // No duplicate references
        if (refs[index]) {
          throw new IAE("Duplicate CLUSTERED BY key: %d", ord);
        }
        refs[index] = true;
      } else if (clusterKey instanceof SqlIdentifier) {
        // Key is an identifier: CLUSTERED BY foo
        SqlIdentifier key = (SqlIdentifier) clusterKey;

        // Only key of the form foo are allowed, not foo.bar
        if (!key.isSimple()) {
          throw new IAE("CLUSTERED BY keys must be a simple name: '%s'", key.toString());
        }

        // The name must match an item in the select list
        String keyName = key.names.get(0);
        // Slow linear search. We assume that there are not many cluster keys.
        int index = fieldNames.indexOf(keyName);
        if (index == -1) {
          throw new IAE("CLUSTERED BY key column '%s' is not valid", keyName);
        }

        // Can't cluster by __time
        if (index == timeColumnIndex) {
          throw new IAE("Do not include %s in the CLUSTERED BY clause: it is managed by PARTITIONED BY", Columns.TIME_COLUMN);
        }

        // No duplicate references.
        if (refs[index]) {
          throw new IAE("Duplicate CLUSTERED BY key: '%s'", keyName);
        }
        refs[index] = true;
      } else {
        // Key is an expression: CLUSTERED BY CEIL(m2)
        selectScope.validateExpr(clusterKey);
      }
    }
  }

  /**
   * Both the catalog and query define clustering. This is allowed as long as they
   * are identical.
   */
  private void verifyQueryClusterByMatchesCatalog(
      final RelRecordType sourceType,
      final List<ClusterKeySpec> keyCols,
      final SqlNodeList clusteredBy
  )
  {
    if (clusteredBy.size() != keyCols.size()) {
      throw clusterKeyMismatchException(keyCols, clusteredBy);
    }
    List<String> fieldNames = sourceType.getFieldNames();
    for (int i = 0; i < clusteredBy.size(); i++) {
      ClusterKeySpec catalogKey = keyCols.get(i);
      SqlNode clusterKey = clusteredBy.get(i);
      boolean desc = false;

      // Check if the key is compound: only occurs for DESC. The ASC
      // case is abstracted away by the parser.
      if (clusterKey instanceof SqlBasicCall) {
        SqlBasicCall basicCall = (SqlBasicCall) clusterKey;
        if (basicCall.getOperator() == SqlStdOperatorTable.DESC) {
          // Cluster key is compound: CLUSTERED BY foo DESC
          // We check only the first element
          clusterKey = ((SqlBasicCall) clusterKey).getOperandList().get(0);
          desc = true;
        }
      }

      // We now have the actual key.
      String queryKeyName;
      if (clusterKey instanceof SqlNumericLiteral) {
        // Key is an ordinal: CLUSTERED BY 2
        // Ordinals are 1-based.
        int ord = ((SqlNumericLiteral) clusterKey).intValue(true);
        queryKeyName = fieldNames.get(ord - 1);
      } else if (clusterKey instanceof SqlIdentifier) {
        queryKeyName = ((SqlIdentifier) clusterKey).getSimple();
      } else {
        throw clusterKeyMismatchException(keyCols, clusteredBy);
      }
      if (!catalogKey.expr().equals(queryKeyName) || catalogKey.desc() != desc) {
        throw clusterKeyMismatchException(keyCols, clusteredBy);
      }
    }
  }

  private RuntimeException clusterKeyMismatchException(List<ClusterKeySpec> keyCols, SqlNodeList clusterKeys)
  {
    // Note the format: keyCols will convert to string with brackets, clusterKeys
    // without, so we add brackets in the format only for clusterKeys
    throw new IAE(
        "CLUSTER BY mismatch. Catalog: %s, query: [%s]",
        keyCols,
        clusterKeys
    );
  }

  private RelDataType validateTargetType(SqlInsert insert, RelRecordType sourceType, DatasourceFacade tableMetadata)
  {
    if (tableMetadata == null) {
      return sourceType;
    }
    final boolean isStrict = tableMetadata.isSealed();
    final List<Map.Entry<String, RelDataType>> fields = new ArrayList<>();
    final List<RelDataTypeField> sourceFields = sourceType.getFieldList();
    for (RelDataTypeField sourceField : sourceFields) {
      String colName = sourceField.getName();
      // Check that there are no unnamed columns in the insert.
      if (UNNAMED_COLUMN_PATTERN.matcher(colName).matches()) {
        throw new IAE(UNNAMED_INGESTION_COLUMN_ERROR);
      }
      ColumnFacade definedCol = tableMetadata.column(colName);
      if (definedCol == null) {
        if (isStrict) {
          throw new IAE(
              "Target column \"%s\".\"%s\" is not defined",
              insert.getTargetTable(),
              colName
          );
        }
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }
      if (!definedCol.hasType()) {
        fields.add(Pair.of(colName, sourceField.getType()));
        continue;
      }
      // TODO: Handle error if type not found
      String sqlTypeName = definedCol.sqlStorageType();
      SqlTypeName sqlType = SqlTypeName.get(sqlTypeName);
      fields.add(Pair.of(colName, typeFactory.createSqlType(sqlType)));
    }
    RelDataType targetType = typeFactory.createStructType(fields);
    checkTypeAssignment(sourceType, targetType, insert);
    return targetType;
  }
}
