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

package org.apache.druid.sql.calcite.parser;

import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nullable;

import java.util.List;

/**
 * Common base class to the two Druid "ingest" statements: INSERT and REPLACE.
 * Allows Planner code to work with these two statements generically where they
 * share common clauses.
 */
public abstract class DruidSqlIngest extends SqlInsert
{
  @Nullable
  protected final SqlNode partitionedBy;

  @Nullable
  protected final SqlNodeList clusteredBy;

  /**
   * One of three values.
   * <ul>
   * <li>{@code null} - No option set. Obtain from catalog or default.</li>
   * <li>Literal {@code true} - Segments include rollup.</li>
   * <li>Literal {@code false} - Segments are detail (no rollup).</li>
   * </ul>
   */
  @Nullable
  final SqlNode rollup;

  public DruidSqlIngest(SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      @Nullable SqlNode partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlNode rollup
  )
  {
    super(pos, keywords, targetTable, source, columnList);

    this.partitionedBy = partitionedBy;
    this.clusteredBy = clusteredBy;
    this.rollup = rollup;
  }

  public SqlNode getPartitionedBy()
  {
    return partitionedBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  @Nullable public SqlNode getRollup()
  {
    return rollup;
  }

  @Nullable public Boolean getRollupOption()
  {
    if (rollup == null) {
      return null;
    }
    return (Boolean) ((SqlLiteral) rollup).getValue();
  }

  @Override
  public List<SqlNode> getOperandList()
  {
    return ImmutableNullableList.<SqlNode>builder()
        .addAll(super.getOperandList())
        .add(partitionedBy)
        .add(clusteredBy)
        .add(rollup)
        .build();
  }

  public void unparseTail(SqlWriter writer, int leftPrec, int rightPrec)
  {
    writer.keyword("PARTITIONED BY");
    writer.keyword(partitionedBy.toString());
    if (getClusteredBy() != null) {
      writer.keyword("CLUSTERED BY");
      SqlWriter.Frame frame = writer.startList("", "");
      for (SqlNode clusterByOpts : getClusteredBy().getList()) {
        clusterByOpts.unparse(writer, leftPrec, rightPrec);
      }
      writer.endList(frame);
    }
    Boolean rollupOption = getRollupOption();
    if (rollupOption != null) {
      writer.keyword(rollupOption ? "WITH" : "WITHOUT");
      writer.keyword("ROLLUP");
    }
  }
}
