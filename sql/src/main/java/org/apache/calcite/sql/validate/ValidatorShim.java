package org.apache.calcite.sql.validate;

import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;

public class ValidatorShim
{
  public static OrderByScope newOrderByScope(
      SqlValidatorScope parent,
      SqlNodeList orderList,
      SqlSelect select)
  {
    return new OrderByScope(parent, orderList, select);
  }
}
