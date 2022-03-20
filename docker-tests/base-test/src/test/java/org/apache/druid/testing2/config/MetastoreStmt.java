package org.apache.druid.testing2.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class MetastoreStmt
{
  private final String sql;

  @JsonCreator
  public MetastoreStmt(
      @JsonProperty("sql") String sql
  )
  {
    this.sql = sql;
  }

  @JsonProperty("sql")
  public String sql()
  {
    return sql;
  }

  @Override
  public String toString()
  {
    return TestConfigs.toYaml(this);
  }

  /**
   * Convert the human-readable form of the statement in YAML
   * into the compact JSON form preferred in the DB. Also
   * compacts the SQL, but that's OK.
   */
  public String toSQL()
  {
    String stmt = sql.replaceAll("\n", " ");
    stmt = stmt.replaceAll(" +", " ");
    stmt = stmt.replaceAll(": ", ":");
    stmt = stmt.replaceAll(", ", ",");
    stmt = stmt.replaceAll(" \\}", "}");
    stmt = stmt.replaceAll("\\{ ", "{");
    return stmt;
  }
}
