package com.tn.query.jdbc;

import static java.lang.String.format;
import static java.util.stream.Collectors.joining;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntSupplier;

import com.tn.query.AbstractQueryParser;
import com.tn.query.Mapper;
import com.tn.query.QueryParseException;

public class JdbcQueryParser extends AbstractQueryParser<JdbcPredicate>
{
  private static final String COMMA = ", ";
  private static final String TEMPLATE_EQUAL = "%s = %s";
  private static final String TEMPLATE_NOT_EQUAL = "NOT %s = %s";
  private static final String TEMPLATE_GREATER_THAN = "%s > %s";
  private static final String TEMPLATE_GREATER_THAN_OR_EQUAL = "%s >= %s";
  private static final String TEMPLATE_LESS_THAN = "%s < %s";
  private static final String TEMPLATE_LESS_THAN_OR_EQUAL = "%s <= %s";
  private static final String TEMPLATE_IN = "%s IN (%s)";
  private static final String TEMPLATE_AND = "%s AND %s";
  private static final String TEMPLATE_OR = "%s OR %s";

  private final Map<String, String> nameMappings;

  private ZoneOffset zoneOffset;

  public JdbcQueryParser(Map<String, String> nameMappings, Collection<Mapper> valueMappers)
  {
    super(valueMappers);
    this.nameMappings = nameMappings;
    this.zoneOffset = ZoneOffset.UTC;
  }

  public void setZoneOffset(ZoneOffset zoneOffset)
  {
    this.zoneOffset = zoneOffset;
  }

  @Override
  protected JdbcPredicate equal(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_EQUAL,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate notEqual(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_NOT_EQUAL,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate greaterThan(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_GREATER_THAN,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate greaterThanOrEqual(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_GREATER_THAN_OR_EQUAL,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate lessThan(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_LESS_THAN,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate lessThanOrEqual(String left, Object right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_LESS_THAN_OR_EQUAL,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate in(String left, List<?> right)
  {
    return new JdbcComparison(
      this.zoneOffset,
      TEMPLATE_IN,
      name(left),
      right
    );
  }

  @Override
  protected JdbcPredicate and(JdbcPredicate left, JdbcPredicate right)
  {
    return new JdbcLogical(TEMPLATE_AND, left, right);
  }

  @Override
  protected JdbcPredicate or(JdbcPredicate left, JdbcPredicate right)
  {
    return new JdbcLogical(TEMPLATE_OR, left, right);
  }

  @Override
  protected JdbcPredicate parenthesis(JdbcPredicate predicate)
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
    private final ZoneOffset zoneOffset;

    public JdbcComparison(ZoneOffset zoneOffset, String template, String name, Object value)
    {
      this.zoneOffset = zoneOffset;
      this.name = name;
      this.value = value;
      this.template = template;
    }

    @Override
    protected void setValues(PreparedStatement preparedStatement, IntSupplier index) throws SQLException
    {
      try
      {
        setValue(preparedStatement, index, this.value);
      }
      catch (RuntimeException e)
      {
        if (e.getCause() instanceof SQLException) throw (SQLException)e.getCause();
        throw e;
      }
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

    private void setValue(PreparedStatement preparedStatement, IntSupplier index, Object value)
    {
      try
      {
        if (value instanceof Boolean) preparedStatement.setBoolean(index.getAsInt(), (Boolean)value);
        else if (value instanceof Byte) preparedStatement.setByte(index.getAsInt(), (Byte)value);
        else if (value instanceof Character) preparedStatement.setString(index.getAsInt(), ((Character)value).toString());
        else if (value instanceof Date) preparedStatement.setTimestamp(index.getAsInt(), toTimestamp((Date)value));
        else if (value instanceof Double) preparedStatement.setDouble(index.getAsInt(), (Double)value);
        else if (value instanceof Float) preparedStatement.setFloat(index.getAsInt(), (Float)value);
        else if (value instanceof Integer) preparedStatement.setInt(index.getAsInt(), (Integer)value);
        else if (value instanceof LocalDate) preparedStatement.setDate(index.getAsInt(), toDate((LocalDate)value));
        else if (value instanceof LocalDateTime) preparedStatement.setTimestamp(index.getAsInt(), toTimestamp((LocalDateTime)value));
        else if (value instanceof Long) preparedStatement.setLong(index.getAsInt(), (Long)value);
        else if (value instanceof Short) preparedStatement.setShort(index.getAsInt(), (Short)value);
        else if (value instanceof Collection) ((Collection<?>)value).forEach(v -> setValue(preparedStatement, index, v));
        else preparedStatement.setObject(index.getAsInt(), value);
      }
      catch (SQLException e)
      {
        throw new RuntimeException(e);
      }
    }

    private String placeholder()
    {
      return this.value instanceof Collection ? ((Collection<?>)value).stream().map(v -> VALUE_PLACEHOLDER).collect(joining(COMMA)) : VALUE_PLACEHOLDER;
    }

    private String valueStr()
    {
      return this.value instanceof Collection ? ((Collection<?>)value).stream().map(Object::toString).collect(joining(COMMA)) : VALUE_PLACEHOLDER;
    }

    private java.sql.Date toDate(LocalDate value)
    {
      return new java.sql.Date(value.atStartOfDay(this.zoneOffset).toInstant().toEpochMilli());
    }

    private Timestamp toTimestamp(Date value)
    {
      return new Timestamp(value.getTime());
    }

    private Timestamp toTimestamp(LocalDateTime value)
    {
      return new Timestamp(value.toInstant(this.zoneOffset).toEpochMilli());
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
