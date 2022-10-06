package com.tn.query.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public interface JdbcPredicate
{
  void setValues(PreparedStatement preparedStatement) throws SQLException;

  String toSql();
}
