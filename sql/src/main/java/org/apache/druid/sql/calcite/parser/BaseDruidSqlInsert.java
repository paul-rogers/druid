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
import org.apache.calcite.sql.SqlNodeList;
import org.apache.druid.java.util.common.granularity.Granularity;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public abstract class BaseDruidSqlInsert extends SqlInsert
{
  protected Granularity partitionedBy;

  // Used in the unparse function to generate the original query
  // since we convert the string to an enum
  protected String partitionedByStringForUnparse;

  @Nullable
  protected SqlNodeList clusteredBy;

  /**
   * While partitionedBy and partitionedByStringForUnparse can be null as arguments to the constructor, this is
   * disallowed (semantically) and the constructor performs checks to ensure that. This helps in producing friendly
   * errors when the PARTITIONED BY custom clause is not present, and keeps its error separate from JavaCC/Calcite's
   * custom errors which can be cryptic when someone accidentally forgets to explicitly specify the PARTITIONED BY clause
   */
  public BaseDruidSqlInsert(
      @Nonnull SqlInsert insertNode,
      @Nullable Granularity partitionedBy,
      @Nullable String partitionedByStringForUnparse,
      @Nullable SqlNodeList clusteredBy
  ) throws ParseException
  {
    super(
        insertNode.getParserPosition(),
        (SqlNodeList) insertNode.getOperandList().get(0), // No better getter to extract this
        insertNode.getTargetTable(),
        insertNode.getSource(),
        insertNode.getTargetColumnList()
    );
    this.partitionedBy = partitionedBy;
    this.partitionedByStringForUnparse = partitionedByStringForUnparse;
    this.clusteredBy = clusteredBy;
  }

  @Nullable
  public SqlNodeList getClusteredBy()
  {
    return clusteredBy;
  }

  public Granularity getPartitionedBy()
  {
    return partitionedBy;
  }

  // This method and the next should not exist. There should be a
  // SqlNode that holds this information, not the parse nodes.
  public void updateParitionedBy(Granularity granularity, String granularityString)
  {
    this.partitionedBy = granularity;
    this.partitionedByStringForUnparse = granularityString;
  }

  public void updateClusteredBy(SqlNodeList clusteredBy)
  {
    this.clusteredBy = clusteredBy;
  }
}
