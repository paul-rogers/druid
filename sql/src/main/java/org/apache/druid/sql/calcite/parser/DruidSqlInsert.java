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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.druid.sql.calcite.planner.DruidSqlIngestOperator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Extends the 'insert' call to hold custom parameters specific to Druid i.e. PARTITIONED BY and CLUSTERED BY
 * This class extends the {@link DruidSqlIngest} so that this SqlNode can be used in
 * {@link org.apache.calcite.sql2rel.SqlToRelConverter} for getting converted into RelNode, and further processing
 */
public class DruidSqlInsert extends DruidSqlIngest
{
  public static final String SQL_INSERT_SEGMENT_GRANULARITY = "sqlInsertSegmentGranularity";

  public DruidSqlInsert(
      @Nonnull SqlInsert insertNode,
      @Nullable SqlNode partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlNode rollup
  )
  {
    this(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList(),
        partitionedBy,
        clusteredBy,
        rollup
    );
  }

  public DruidSqlInsert(
      SqlParserPos pos,
      SqlNodeList keywords,
      SqlNode targetTable,
      SqlNode source,
      SqlNodeList columnList,
      @Nullable SqlNode partitionedBy,
      @Nullable SqlNodeList clusteredBy,
      @Nullable SqlNode rollup
  )
  {
    super(
        pos,
        keywords,
        targetTable,
        source,
        columnList,
        partitionedBy,
        clusteredBy,
        rollup
    );
  }

  @Nonnull
  @Override
  public SqlOperator getOperator()
  {
    return DruidSqlIngestOperator.INSERT_OPERATOR;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec)
  {
    super.unparse(writer, leftPrec, rightPrec);
    unparseTail(writer, leftPrec, rightPrec);
  }
}
