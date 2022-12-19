package org.apache.druid.sql.calcite.planner;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.druid.math.expr.ExprMacroTable;
import org.apache.druid.segment.join.JoinableFactoryWrapper;
import org.apache.druid.sql.calcite.schema.DruidSchemaCatalog;

public interface PlannerToolbox
{
  DruidOperatorTable operatorTable();

  ExprMacroTable exprMacroTable();

  ObjectMapper jsonMapper();

  PlannerConfig plannerConfig();

  DruidSchemaCatalog rootSchema();

  JoinableFactoryWrapper joinableFactoryWrapper();

  CatalogResolver catalogResolver();

  String druidSchemaName();
}
