package org.apache.druid.sql.calcite.planner;

import com.google.common.collect.ImmutableSet;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlInsert;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.druid.server.security.Action;
import org.apache.druid.server.security.Resource;
import org.apache.druid.server.security.ResourceAction;
import org.apache.druid.server.security.ResourceType;
import org.apache.druid.sql.calcite.expression.AuthorizableOperator;
import org.apache.druid.sql.calcite.parser.DruidSqlIngest;

import java.util.Set;

public class DruidSqlIngestOperator extends SqlSpecialOperator implements AuthorizableOperator
{
  public static final SqlSpecialOperator INSERT_OPERATOR =
      new DruidSqlIngestOperator("INSERT", SqlKind.INSERT);
  public static final SqlSpecialOperator REPLACE_OPERATOR =
      new DruidSqlIngestOperator("REPLACE", SqlKind.INSERT);

  public DruidSqlIngestOperator(String name, SqlKind kind)
  {
    super(name, kind);
  }

  @Override
  public Set<ResourceAction> computeResources(SqlCall call)
  {
    DruidSqlIngest ingestNode = (DruidSqlIngest) call;
    final SqlIdentifier tableIdentifier = (SqlIdentifier) ingestNode.getTargetTable();
    String targetDatasource = tableIdentifier.names.get(tableIdentifier.names.size() - 1);
    return ImmutableSet.of(new ResourceAction(new Resource(targetDatasource, ResourceType.DATASOURCE), Action.WRITE));
  }
}
