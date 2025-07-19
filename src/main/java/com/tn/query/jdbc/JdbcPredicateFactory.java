package com.tn.query.jdbc;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import com.tn.lang.sql.PreparedStatements;
import com.tn.query.PredicateFactory;
import com.tn.query.QueryException;
import com.tn.query.QueryParseException;

public class JdbcPredicateFactory implements PredicateFactory<JdbcPredicate>
{
  private static final String COMMA = ", ";
  private static final String LIKE_WILDCARD = "%";
  private static final String TEMPLATE_EQUAL = "%s = %s";
  private static final String TEMPLATE_NOT_EQUAL = "NOT %s = %s";
  private static final String TEMPLATE_NULL = "%s IS NULL";
  private static final String TEMPLATE_NOT_NULL = "%s IS NOT NULL";
  private static final String TEMPLATE_GREATER_THAN = "%s > %s";
  private static final String TEMPLATE_GREATER_THAN_OR_EQUAL = "%s >= %s";
  private static final String TEMPLATE_LESS_THAN = "%s < %s";
  private static final String TEMPLATE_LESS_THAN_OR_EQUAL = "%s <= %s";
  private static final String TEMPLATE_LIKE = "%s LIKE %s";
  private static final String TEMPLATE_NOT_LIKE = "%s NOT LIKE %s";
  private static final String TEMPLATE_IN = "%s IN (%s)";
  private static final String TEMPLATE_AND = "%s AND %s";
  private static final String TEMPLATE_OR = "%s OR %s";
  private static final String WILDCARD = "*";

  private final Map<String, String> nameMappings;

  public JdbcPredicateFactory(Map<String, String> nameMappings)
  {
    this.nameMappings = nameMappings;
  }

  @Override
  public JdbcPredicate equal(String left, Object right)
  {
    return new JdbcComparison(
      right != null ? TEMPLATE_EQUAL : TEMPLATE_NULL,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate notEqual(String left, Object right)
  {
    return new JdbcComparison(
      right != null ? TEMPLATE_NOT_EQUAL : TEMPLATE_NOT_NULL,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate greaterThan(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_GREATER_THAN,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate greaterThanOrEqual(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_GREATER_THAN_OR_EQUAL,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate lessThan(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_LESS_THAN,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate lessThanOrEqual(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_LESS_THAN_OR_EQUAL,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate like(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_LIKE,
      name(left),
      replaceWildcard(right)
    );
  }

  @Override
  public JdbcPredicate notLike(String left, Object right)
  {
    return new JdbcComparison(
      TEMPLATE_NOT_LIKE,
      name(left),
      replaceWildcard(right)
    );
  }

  @Override
  public JdbcPredicate in(String left, List<?> right)
  {
    return new JdbcComparison(
      TEMPLATE_IN,
      name(left),
      right
    );
  }

  @Override
  public JdbcPredicate and(JdbcPredicate left, JdbcPredicate right)
  {
    return new JdbcLogical(TEMPLATE_AND, left, right);
  }

  @Override
  public JdbcPredicate or(JdbcPredicate left, JdbcPredicate right)
  {
    return new JdbcLogical(TEMPLATE_OR, left, right);
  }

  @Override
  public JdbcPredicate parenthesis(JdbcPredicate predicate)
  {
    if (!(predicate instanceof AbstractJdbcPredicate)) throw new QueryParseException("Expected AbstractJdbcPredicate");
    else return new JdbcParenthesis((AbstractJdbcPredicate)predicate);
  }

  private String name(String left)
  {
    String name = this.nameMappings.get(left);
    if (name == null) throw new QueryParseException("Unknown name: " + left);

    return name;
  }

  private Object replaceWildcard(Object value)
  {
    if (!(value instanceof String)) throw new QueryException("Like comparisons only work for string values, received: " + value);

    return value.toString().replace(WILDCARD, LIKE_WILDCARD);
  }

  private static abstract class AbstractJdbcPredicate implements JdbcPredicate
  {
    private static final int INITIAL_INDEX = 1;

    @Override
    public final void setValues(PreparedStatement preparedStatement) throws SQLException
    {
      setValues(preparedStatement, new AtomicInteger(INITIAL_INDEX)::getAndIncrement);
    }

    protected abstract void setValues(PreparedStatement preparedStatement, IntSupplier index) throws SQLException;
  }

  private static class JdbcComparison extends AbstractJdbcPredicate
  {
    private static final String VALUE_PLACEHOLDER = "?";

    private final String name;
    private final String template;
    private final Object value;

    public JdbcComparison(String template, String name, Object value)
    {
      this.name = name;
      this.value = value;
      this.template = template;
    }

    @Override
    protected void setValues(PreparedStatement preparedStatement, IntSupplier index) throws SQLException
    {
      PreparedStatements.setValue(preparedStatement, index, this.value);
    }

    @Override
    public String toSql()
    {
      return format(this.template, this.name, placeholder());
    }

    @Override
    public String toString()
    {
      return format(this.template, this.name, valueStr());
    }

    private String placeholder()
    {
      return this.value instanceof Collection ? ((Collection<?>)value).stream().map(v -> VALUE_PLACEHOLDER).collect(joining(COMMA)) : VALUE_PLACEHOLDER;
    }

    private String valueStr()
    {
      return this.value instanceof Collection ? ((Collection<?>)value).stream().map(Object::toString).collect(joining(COMMA)) : String.valueOf(this.value);
    }
  }

  private static class JdbcLogical extends AbstractJdbcPredicate
  {
    private final String template;
    private final JdbcPredicate left;
    private final JdbcPredicate right;

    public JdbcLogical(String template, JdbcPredicate left, JdbcPredicate right)
    {
      this.template = template;
      this.left = left;
      this.right = right;
    }

    @Override
    public String toSql()
    {
      return format(this.template, this.left.toSql(), this.right.toSql());
    }

    @Override
    public String toString()
    {
      return format(this.template, this.left.toString(), this.right.toString());
    }

    @Override
    protected void setValues(PreparedStatement preparedStatement, IntSupplier index) throws SQLException
    {
      ((AbstractJdbcPredicate)this.left).setValues(preparedStatement, index);
      ((AbstractJdbcPredicate)this.right).setValues(preparedStatement, index);
    }
  }

  private static class JdbcParenthesis extends AbstractJdbcPredicate
  {
    private static final String TEMPLATE_PARENTHESIS = "(%s)";
    private final AbstractJdbcPredicate predicate;

    public JdbcParenthesis(AbstractJdbcPredicate predicate)
    {
      this.predicate = predicate;
    }

    @Override
    public String toSql()
    {
      return format(TEMPLATE_PARENTHESIS, this.predicate.toSql());
    }

    @Override
    public String toString()
    {
      return format(TEMPLATE_PARENTHESIS, this.predicate.toString());
    }

    @Override
    protected void setValues(PreparedStatement preparedStatement, IntSupplier index) throws SQLException
    {
      this.predicate.setValues(preparedStatement, index);
    }
  }
}
