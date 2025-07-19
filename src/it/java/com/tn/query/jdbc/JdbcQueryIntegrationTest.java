package com.tn.query.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneOffset;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.tn.query.DefaultQueryParser;
import com.tn.query.Mapper;
import com.tn.query.QueryException;
import com.tn.query.QueryParseException;
import com.tn.query.QueryParser;

@SuppressWarnings({"SqlResolve", "SqlNoDataSourceInspection"})
class JdbcQueryIntegrationTest
{
  private static final String CREATE_TABLE_SCRIPT = "create-table.sql";
  private static final String DATABASE_URL = "jdbc:h2:mem:mydb";
  private static final String PASSWORD = "";
  private static final String SPACE = " ";
  private static final String USERNAME = "sa";

  private static final Map<String, String> FIELD_MAPPINGS = new HashMap<>();
  static
  {
    FIELD_MAPPINGS.put("booleanValue", "boolean_value");
    FIELD_MAPPINGS.put("byteValue", "byte_value");
    FIELD_MAPPINGS.put("charValue", "char_value");
    FIELD_MAPPINGS.put("dateValue", "date_value");
    FIELD_MAPPINGS.put("doubleValue", "double_value");
    FIELD_MAPPINGS.put("floatValue", "float_value");
    FIELD_MAPPINGS.put("intValue", "int_value");
    FIELD_MAPPINGS.put("localDateValue", "local_date_value");
    FIELD_MAPPINGS.put("localDateTimeValue", "local_date_time_value");
    FIELD_MAPPINGS.put("longValue", "long_value");
    FIELD_MAPPINGS.put("shortValue", "short_value");
    FIELD_MAPPINGS.put("stringValue", "string_value");
  }

  private static final String SQL_INSERT = "INSERT INTO Target(" +
    "boolean_value, " +
    "byte_value, " +
    "char_value, " +
    "date_value, " +
    "double_value, " +
    "float_value, " +
    "int_value, " +
    "local_date_value, " +
    "local_date_time_value, " +
    "long_value, " +
    "short_value, " +
    "string_value" +
  ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

  private static final String SQL_SELECT = "SELECT * FROM Target WHERE ";

  @SuppressWarnings("SqlWithoutWhere")
  private static final String SQL_DELETE = "DELETE FROM Target";

  private static Connection connection;

  private final QueryParser<JdbcPredicate> queryParser = new DefaultQueryParser<>(
    new JdbcPredicateFactory(FIELD_MAPPINGS),
    List.of(
      Mapper.toBoolean("booleanValue"),
      Mapper.toByte("byteValue"),
      Mapper.toChar("charValue"),
      Mapper.toDate("dateValue"),
      Mapper.toDouble("doubleValue"),
      Mapper.toFloat("floatValue"),
      Mapper.toInt("intValue"),
      Mapper.toLocalDate("localDateValue"),
      Mapper.toLocalDateTime("localDateTimeValue"),
      Mapper.toLong("longValue"),
      Mapper.toShort("shortValue")
    )
  );

  @BeforeAll
  static void startDatabase() throws Exception
  {
    //DriverManager.registerDriver(org.h2.Driver.class);
    connection = DriverManager.getConnection(DATABASE_URL, USERNAME, PASSWORD);
    createTable();
  }

  @AfterAll
  static void stopDatabase() throws SQLException
  {
    if (connection != null && !connection.isClosed()) connection.close();
  }

  @AfterEach
  @BeforeEach
  void deleteTarget() throws Exception
  {
    try (Statement statement = connection.createStatement())
    {
      statement.execute(SQL_DELETE);
    }
  }

  @Test
  void shouldMatchBoolean() throws SQLException
  {
    Target target = new Target();
    target.booleanValue = true;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertTrue(resultSet.getBoolean("boolean_value"));

    assertMatch("booleanValue = true", assertion);
    assertNoMatch("booleanValue != true");
    assertNoMatch("booleanValue = X");

    assertNoMatch("booleanValue != true");
    assertMatch("booleanValue != false", assertion);
    assertMatch("booleanValue != X", assertion);

    assertNoMatch("booleanValue > true");
    assertMatch("booleanValue > false", assertion);
    assertMatch("booleanValue > X", assertion);

    assertMatch("booleanValue >= true", assertion);
    assertMatch("booleanValue >= false", assertion);
    assertMatch("booleanValue >= X", assertion);

    assertNoMatch("booleanValue < true");
    assertNoMatch("booleanValue < false");
    assertNoMatch("booleanValue < X");

    assertMatch("booleanValue <= true", assertion);
    assertNoMatch("booleanValue <= false");
    assertNoMatch("booleanValue <= X");

    assertMatch("booleanValue ∈ [true, false]", assertion);
    assertMatch("booleanValue ∈ [true]", assertion);
    assertNoMatch("booleanValue ∈ [false]");
    assertNoMatch("booleanValue ∈ X");

    assertThrows(QueryException.class, () -> this.queryParser.parse("booleanValue ≈ true"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("booleanValue !≈ false"));
  }

  @Test
  void shouldMatchByte() throws SQLException
  {
    Target target = new Target();
    target.byteValue = 1;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.byteValue, resultSet.getByte("byte_value"));

    assertMatch("byteValue = 1", assertion);
    assertNoMatch("byteValue = 2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue = X"));

    assertNoMatch("byteValue != 1");
    assertMatch("byteValue != 2", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue != X"));

    assertMatch("byteValue > 0", assertion);
    assertNoMatch("byteValue > 1");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue > X"));

    assertMatch("byteValue >= 0", assertion);
    assertMatch("byteValue >= 1", assertion);
    assertNoMatch("byteValue >= 2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue >= X"));

    assertMatch("byteValue < 2", assertion);
    assertNoMatch("byteValue < 1");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue < X"));

    assertMatch("byteValue <= 2", assertion);
    assertMatch("byteValue <= 1", assertion);
    assertNoMatch("byteValue <= 0");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue <= X"));

    assertMatch("byteValue ∈ [1, 2]", assertion);
    assertMatch("byteValue ∈ [1]", assertion);
    assertNoMatch("byteValue ∈ [2]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue ≈ 1"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("byteValue !≈ 2"));
  }

  @Test
  void shouldMatchChar() throws SQLException
  {
    Target target = new Target();
    target.charValue = 'b';

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.charValue, resultSet.getString("char_value").charAt(0));
    
    assertMatch("charValue = b", assertion);
    assertNoMatch("charValue = c");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue = bc"));

    assertNoMatch("charValue != b");
    assertMatch("charValue != c", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue != ab"));

    assertMatch("charValue > a", assertion);
    assertNoMatch("charValue > b");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue > ab"));

    assertMatch("charValue >= a", assertion);
    assertMatch("charValue >= b", assertion);
    assertNoMatch("charValue >= c");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue >= ab"));

    assertMatch("charValue < c", assertion);
    assertNoMatch("charValue < b");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue < ab"));

    assertMatch("charValue <= c", assertion);
    assertMatch("charValue <= b", assertion);
    assertNoMatch("charValue <= a");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue <= ab"));


    assertMatch("charValue ∈ [a, b, c]", assertion);
    assertMatch("charValue ∈ [b]", assertion);
    assertNoMatch("charValue ∈ [a]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue ∈ ab"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue ≈ b"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("charValue !≈ c"));
  }

  @Test
  void shouldMatchDate() throws SQLException
  {
    Calendar calendar = Calendar.getInstance();
    calendar.set(2021, Calendar.FEBRUARY, 5, 0, 0, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    Target target = new Target();
    target.dateValue = calendar.getTime();

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.dateValue, resultSet.getDate("date_value"));

    assertMatch("dateValue = 2021-02-05", assertion);
    assertNoMatch("dateValue = 2021-10-06");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue = X"));

    assertNoMatch("dateValue != 2021-02-05");
    assertMatch("dateValue != 2021-10-06", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue != X"));

    assertMatch("dateValue > 2021-02-04", assertion);
    assertNoMatch("dateValue > 2021-02-05");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue > X"));

    assertMatch("dateValue >= 2021-02-04", assertion);
    assertMatch("dateValue >= 2021-02-05", assertion);
    assertNoMatch("dateValue >= 2021-02-06");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue >= X"));

    assertMatch("dateValue < 2021-02-06", assertion);
    assertNoMatch("dateValue < 2021-02-05");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue < X"));

    assertMatch("dateValue <= 2021-02-05", assertion);
    assertMatch("dateValue <= 2021-02-06", assertion);
    assertNoMatch("dateValue <= 2021-02-04");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue <= X"));

    assertMatch("dateValue ∈ [2021-02-04, 2021-02-05, 2021-02-06]", assertion);
    assertMatch("dateValue ∈ [2021-02-05]", assertion);
    assertNoMatch("dateValue ∈ [2021-02-04]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ≈ 2021-02-05"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue !≈ 2021-02-06"));
  }

  @Test
  void shouldMatchDateTimeWithMinutes() throws SQLException
  {
    Calendar calendar = Calendar.getInstance();
    calendar.set(2021, Calendar.FEBRUARY, 5, 10, 15, 0);
    calendar.set(Calendar.MILLISECOND, 0);

    Target target = new Target();
    target.dateValue = calendar.getTime();

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.dateValue, resultSet.getTimestamp("date_value"));

    assertMatch("dateValue = 2021-02-05T10:15", assertion);
    assertNoMatch("dateValue = 2021-02-05T10:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue = X"));

    assertNoMatch("dateValue != 2021-02-05T10:15");
    assertMatch("dateValue != 2021-10-05T10:14", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue != X"));

    assertMatch("dateValue > 2021-02-05T10:14", assertion);
    assertNoMatch("dateValue > 2021-02-05T10:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue > X"));

    assertMatch("dateValue >= 2021-02-05T10:14", assertion);
    assertMatch("dateValue >= 2021-02-05T10:15", assertion);
    assertNoMatch("dateValue >= 2021-02-05T10:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue >= X"));

    assertMatch("dateValue < 2021-02-05T10:16", assertion);
    assertNoMatch("dateValue < 2021-02-05T10:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue < X"));

    assertMatch("dateValue <= 2021-02-05T10:16", assertion);
    assertMatch("dateValue <= 2021-02-05T10:15", assertion);
    assertNoMatch("dateValue <= 2021-02-05T10:14");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue <= X"));


    assertMatch("dateValue ∈ [2021-02-05T10:14, 2021-02-05T10:15, 2021-02-05T10:16]", assertion);
    assertMatch("dateValue ∈ [2021-02-05T10:15]", assertion);
    assertNoMatch("dateValue ∈ [2021-02-05T10:14]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ≈ 2021-02-05T10:15"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue !≈ 2021-02-05T10:16"));
  }

  @Test
  void shouldMatchDateTimeWithSeconds() throws SQLException
  {
    Calendar calendar = Calendar.getInstance();
    calendar.set(2021, Calendar.FEBRUARY, 5, 10, 15, 16);
    calendar.set(Calendar.MILLISECOND, 0);

    Target target = new Target();
    target.dateValue = calendar.getTime();

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.dateValue, resultSet.getTimestamp("date_value"));

    assertMatch("dateValue = 2021-02-05T10:15:16", assertion);
    assertNoMatch("dateValue = 2021-02-05T10:15:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue = X"));

    assertNoMatch("dateValue != 2021-02-05T10:15:16");
    assertMatch("dateValue != 2021-10-05T10:15:15", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue != X"));

    assertMatch("dateValue > 2021-02-05T10:15:15", assertion);
    assertNoMatch("dateValue > 2021-02-05T10:15:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue > X"));

    assertMatch("dateValue >= 2021-02-05T10:15:15", assertion);
    assertMatch("dateValue >= 2021-02-05T10:15:16", assertion);
    assertNoMatch("dateValue >= 2021-02-05T10:15:17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue >= X"));

    assertMatch("dateValue < 2021-02-05T10:15:17", assertion);
    assertNoMatch("dateValue < 2021-02-05T10:15:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue < X"));

    assertMatch("dateValue <= 2021-02-05T10:15:17", assertion);
    assertMatch("dateValue <= 2021-02-05T10:15:16", assertion);
    assertNoMatch("dateValue <= 2021-02-05T10:15:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue <= X"));


    assertMatch("dateValue ∈ [2021-02-05T10:15:15, 2021-02-05T10:15:16, 2021-02-05T10:15:17]", assertion);
    assertMatch("dateValue ∈ [2021-02-05T10:15:16]", assertion);
    assertNoMatch("dateValue ∈ [2021-02-05T10:15:17]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ≈ 2021-02-05T10:15:16"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue !≈ 2021-02-05T10:17"));
  }

  @Test
  void shouldMatchDateTimeWithMilliseconds() throws SQLException
  {
    Calendar calendar = Calendar.getInstance();
    calendar.set(2021, Calendar.FEBRUARY, 5, 10, 15, 16);
    calendar.set(Calendar.MILLISECOND, 17);

    Target target = new Target();
    target.dateValue = calendar.getTime();

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.dateValue, resultSet.getTimestamp("date_value"));

    assertMatch("dateValue = 2021-02-05T10:15:16.17", assertion);
    assertNoMatch("dateValue = 2021-02-05T10:15:16.18");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue = X"));

    assertNoMatch("dateValue != 2021-02-05T10:15:16.17");
    assertMatch("dateValue != 2021-10-05T10:15:16.16", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue != X"));

    assertMatch("dateValue > 2021-02-05T10:15:16.16", assertion);
    assertNoMatch("dateValue > 2021-02-05T10:15:16.17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue > X"));

    assertMatch("dateValue >= 2021-02-05T10:15:16.16", assertion);
    assertMatch("dateValue >= 2021-02-05T10:15:16.17", assertion);
    assertNoMatch("dateValue >= 2021-02-05T10:15:16.18");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue >= X"));

    assertMatch("dateValue < 2021-02-05T10:15:16.18", assertion);
    assertNoMatch("dateValue < 2021-02-05T10:15:16.17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue < X"));

    assertMatch("dateValue <= 2021-02-05T10:15:16.17", assertion);
    assertMatch("dateValue <= 2021-02-05T10:15:16.18", assertion);
    assertNoMatch("dateValue <= 2021-02-05T10:15:16.16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue <= X"));


    assertMatch("dateValue ∈ [2021-02-05T10:15:16.16, 2021-02-05T10:15:16.17, 2021-02-05T10:15:16.18]", assertion);
    assertMatch("dateValue ∈ [2021-02-05T10:15:16.17]", assertion);
    assertNoMatch("dateValue ∈ [2021-02-05T10:15:16.16]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue ≈ 2021-02-05T10:15:16.17"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("dateValue !≈ 2021-02-05T10:15:16.18"));
  }

  @Test
  void shouldMatchDouble() throws SQLException
  {
    Target target = new Target();
    target.doubleValue = 1.2;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.doubleValue, resultSet.getDouble("double_value"));

    assertMatch("doubleValue = 1.2", assertion);
    assertNoMatch("doubleValue = 1.3");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue = X"));

    assertNoMatch("doubleValue != 1.2");
    assertMatch("doubleValue != 1.3", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue != X"));

    assertMatch("doubleValue > 1.1", assertion);
    assertNoMatch("doubleValue > 1.2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue > X"));

    assertMatch("doubleValue >= 1.1", assertion);
    assertMatch("doubleValue >= 1.2", assertion);
    assertNoMatch("doubleValue >= 1.3");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue >= X"));

    assertMatch("doubleValue < 1.3", assertion);
    assertNoMatch("doubleValue < 1.2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue < X"));

    assertMatch("doubleValue <= 1.3", assertion);
    assertMatch("doubleValue <= 1.2", assertion);
    assertNoMatch("doubleValue <= 1.1");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue <= X"));


    assertMatch("doubleValue ∈ [1.1, 1.2, 1.3]", assertion);
    assertMatch("doubleValue ∈ [1.2]", assertion);
    assertNoMatch("doubleValue ∈ [1.1]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue ≈ 1.2"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("doubleValue !≈ 1.3"));
  }

  @Test
  void shouldMatchFloat() throws SQLException
  {
    Target target = new Target();
    target.floatValue = 1.2F;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.floatValue, resultSet.getFloat("float_value"));

    assertMatch("floatValue = 1.2", assertion);
    assertNoMatch("floatValue = 1.3");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue = X"));

    assertNoMatch("floatValue != 1.2");
    assertMatch("floatValue != 1.3", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue != X"));

    assertMatch("floatValue > 1.1", assertion);
    assertNoMatch("floatValue > 1.2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue > X"));

    assertMatch("floatValue >= 1.1", assertion);
    assertMatch("floatValue >= 1.2", assertion);
    assertNoMatch("floatValue >= 1.3");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue >= X"));

    assertMatch("floatValue < 1.3", assertion);
    assertNoMatch("floatValue < 1.2");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue < X"));

    assertMatch("floatValue <= 1.3", assertion);
    assertMatch("floatValue <= 1.2", assertion);
    assertNoMatch("floatValue <= 1.1");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue <= X"));

    assertMatch("floatValue ∈ [1.1, 1.2, 1.3]", assertion);
    assertMatch("floatValue ∈ [1.2]", assertion);
    assertNoMatch("floatValue ∈ [1.1]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue ≈ 1.2"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("floatValue !≈ 1.3"));
  }


  @Test
  void shouldMatchInt() throws SQLException
  {
    Target target = new Target();
    target.intValue = 10;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.intValue, resultSet.getInt("int_value"));

    assertMatch("intValue = 10", assertion);
    assertNoMatch("intValue = 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue = X"));

    assertNoMatch("intValue != 10");
    assertMatch("intValue != 11", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue != X"));

    assertMatch("intValue > 9", assertion);
    assertNoMatch("intValue > 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue > X"));

    assertMatch("intValue >= 9", assertion);
    assertMatch("intValue >= 10", assertion);
    assertNoMatch("intValue >= 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue >= X"));

    assertMatch("intValue < 11", assertion);
    assertNoMatch("intValue < 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue < X"));

    assertMatch("intValue <= 11", assertion);
    assertMatch("intValue <= 10", assertion);
    assertNoMatch("intValue <= 9");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue <= X"));

    assertMatch("intValue ∈ [9, 10, 11]", assertion);
    assertMatch("intValue ∈ [10]", assertion);
    assertNoMatch("intValue ∈ [9]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue !≈ 11"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("intValue ≈ 10"));
  }

  @Test
  void shouldMatchLocalDate() throws SQLException
  {
    LocalDate localDate = LocalDate.of(2021, Month.FEBRUARY, 5);

    Target target = new Target();
    target.localDateValue = localDate;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.localDateValue, resultSet.getDate("local_date_value").toLocalDate());

    assertMatch("localDateValue = 2021-02-05", assertion);
    assertNoMatch("localDateValue = 2021-10-06");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue = X"));

    assertNoMatch("localDateValue != 2021-02-05");
    assertMatch("localDateValue != 2021-10-06", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue != X"));

    assertMatch("localDateValue > 2021-02-04", assertion);
    assertNoMatch("localDateValue > 2021-02-05");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue > X"));

    assertMatch("localDateValue >= 2021-02-04", assertion);
    assertMatch("localDateValue >= 2021-02-05", assertion);
    assertNoMatch("localDateValue >= 2021-02-06");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue >= X"));

    assertMatch("localDateValue < 2021-02-06", assertion);
    assertNoMatch("localDateValue < 2021-02-05");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue < X"));

    assertMatch("localDateValue <= 2021-02-05", assertion);
    assertMatch("localDateValue <= 2021-02-06", assertion);
    assertNoMatch("localDateValue <= 2021-02-04");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue <= X"));


    assertMatch("localDateValue ∈ [2021-02-04, 2021-02-05, 2021-02-06]", assertion);
    assertMatch("localDateValue ∈ [2021-02-05]", assertion);
    assertNoMatch("localDateValue ∈ [2021-02-04]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue ≈ 2021-02-06"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateValue !≈ 2021-02-07"));
  }

  @Test
  void shouldMatchLocalDateTimeWithMinutes() throws SQLException
  {
    LocalDateTime localDateTime = LocalDateTime.of(2021, Month.FEBRUARY, 5, 10, 15);

    Target target = new Target();
    target.localDateTimeValue = localDateTime;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.localDateTimeValue, resultSet.getTimestamp("local_date_time_value").toLocalDateTime());

    assertMatch("localDateTimeValue = 2021-02-05T10:15", assertion);
    assertNoMatch("localDateTimeValue = 2021-02-05T10:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue = X"));

    assertNoMatch("localDateTimeValue != 2021-02-05T10:15");
    assertMatch("localDateTimeValue != 2021-10-05T10:14", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue != X"));

    assertMatch("localDateTimeValue > 2021-02-05T10:14", assertion);
    assertNoMatch("localDateTimeValue > 2021-02-05T10:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue > X"));

    assertMatch("localDateTimeValue >= 2021-02-05T10:14", assertion);
    assertMatch("localDateTimeValue >= 2021-02-05T10:15", assertion);
    assertNoMatch("localDateTimeValue >= 2021-02-05T10:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue >= X"));

    assertMatch("localDateTimeValue < 2021-02-05T10:16", assertion);
    assertNoMatch("localDateTimeValue < 2021-02-05T10:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue < X"));

    assertMatch("localDateTimeValue <= 2021-02-05T10:16", assertion);
    assertMatch("localDateTimeValue <= 2021-02-05T10:15", assertion);
    assertNoMatch("localDateTimeValue <= 2021-02-05T10:14");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue <= X"));

    assertMatch("localDateTimeValue ∈ [2021-02-05T10:14, 2021-02-05T10:15, 2021-02-05T10:16]", assertion);
    assertMatch("localDateTimeValue ∈ [2021-02-05T10:15]", assertion);
    assertNoMatch("localDateTimeValue ∈ [2021-02-05T10:14]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ≈ 2021-02-05T10:16"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue !≈ 2021-02-05T10:15"));
  }

  @Test
  void shouldMatchLocalDateTimeWithSeconds() throws SQLException
  {
    LocalDateTime localDateTime = LocalDateTime.of(2021, Month.FEBRUARY, 5, 10, 15, 16);

    Target target = new Target();
    target.localDateTimeValue = localDateTime;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.localDateTimeValue, resultSet.getTimestamp("local_date_time_value").toLocalDateTime());

    assertMatch("localDateTimeValue = 2021-02-05T10:15:16", assertion);
    assertNoMatch("localDateTimeValue = 2021-02-05T10:15:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue = X"));

    assertNoMatch("localDateTimeValue != 2021-02-05T10:15:16");
    assertMatch("localDateTimeValue != 2021-10-05T10:15:15", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue != X"));

    assertMatch("localDateTimeValue > 2021-02-05T10:15:15", assertion);
    assertNoMatch("localDateTimeValue > 2021-02-05T10:15:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue > X"));

    assertMatch("localDateTimeValue >= 2021-02-05T10:15:15", assertion);
    assertMatch("localDateTimeValue >= 2021-02-05T10:15:16", assertion);
    assertNoMatch("localDateTimeValue >= 2021-02-05T10:15:17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue >= X"));

    assertMatch("localDateTimeValue < 2021-02-05T10:15:17", assertion);
    assertNoMatch("localDateTimeValue < 2021-02-05T10:15:16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue < X"));

    assertMatch("localDateTimeValue <= 2021-02-05T10:15:17", assertion);
    assertMatch("localDateTimeValue <= 2021-02-05T10:15:16", assertion);
    assertNoMatch("localDateTimeValue <= 2021-02-05T10:15:15");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue <= X"));

    assertMatch("localDateTimeValue ∈ [2021-02-05T10:15:15, 2021-02-05T10:15:16, 2021-02-05T10:15:17]", assertion);
    assertMatch("localDateTimeValue ∈ [2021-02-05T10:15:16]", assertion);
    assertNoMatch("localDateTimeValue ∈ [2021-02-05T10:15:17]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ≈ 2021-02-05T10:15:16"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue !≈ 2021-02-05T10:15:15"));
  }

  @Test
  void shouldMatchLocalDateTimeWithMilliseconds() throws SQLException
  {
    LocalDateTime localDateTime = LocalDateTime.of(2021, Month.FEBRUARY, 5, 10, 15, 16, 170_000_000);

    Target target = new Target();
    target.localDateTimeValue = localDateTime;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.localDateTimeValue, resultSet.getTimestamp("local_date_time_value").toLocalDateTime());

    assertMatch("localDateTimeValue = 2021-02-05T10:15:16.17", assertion);
    assertNoMatch("localDateTimeValue = 2021-02-05T10:15:16.18");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue = X"));

    assertNoMatch("localDateTimeValue != 2021-02-05T10:15:16.17");
    assertMatch("localDateTimeValue != 2021-10-05T10:15:16.16", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue != X"));

    assertMatch("localDateTimeValue > 2021-02-05T10:15:16.16", assertion);
    assertNoMatch("localDateTimeValue > 2021-02-05T10:15:16.17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue > X"));

    assertMatch("localDateTimeValue >= 2021-02-05T10:15:16.16", assertion);
    assertMatch("localDateTimeValue >= 2021-02-05T10:15:16.17", assertion);
    assertNoMatch("localDateTimeValue >= 2021-02-05T10:15:16.18");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue >= X"));

    assertMatch("localDateTimeValue < 2021-02-05T10:15:16.18", assertion);
    assertNoMatch("localDateTimeValue < 2021-02-05T10:15:16.17");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue < X"));

    assertMatch("localDateTimeValue <= 2021-02-05T10:15:16.17", assertion);
    assertMatch("localDateTimeValue <= 2021-02-05T10:15:16.18", assertion);
    assertNoMatch("localDateTimeValue <= 2021-02-05T10:15:16.16");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue <= X"));


    assertMatch("localDateTimeValue ∈ [2021-02-05T10:15:16.16, 2021-02-05T10:15:16.17, 2021-02-05T10:15:16.18]", assertion);
    assertMatch("localDateTimeValue ∈ [2021-02-05T10:15:16.17]", assertion);
    assertNoMatch("localDateTimeValue ∈ [2021-02-05T10:15:16.16]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue ≈ 2021-02-05T10:15:16.18"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("localDateTimeValue !≈ 2021-02-05T10:15:16.17"));
  }

  @Test
  void shouldMatchLong() throws SQLException
  {
    Target target = new Target();
    target.longValue = 10L;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.longValue, resultSet.getLong("long_value"));

    assertMatch("longValue = 10", assertion);
    assertNoMatch("longValue = 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue = X"));

    assertNoMatch("longValue != 10");
    assertMatch("longValue != 11", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue != X"));

    assertMatch("longValue > 9", assertion);
    assertNoMatch("longValue > 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue > X"));

    assertMatch("longValue >= 9", assertion);
    assertMatch("longValue >= 10", assertion);
    assertNoMatch("longValue >= 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue >= X"));

    assertMatch("longValue < 11", assertion);
    assertNoMatch("longValue < 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue < X"));

    assertMatch("longValue <= 11", assertion);
    assertMatch("longValue <= 10", assertion);
    assertNoMatch("longValue <= 9");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue <= X"));

    assertMatch("longValue ∈ [9, 10, 11]", assertion);
    assertMatch("longValue ∈ [10]", assertion);
    assertNoMatch("longValue ∈ [9]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue ≈ 11"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("longValue !≈ 10"));
  }

  @Test
  void shouldMatchShort() throws SQLException
  {
    Target target = new Target();
    target.shortValue = 10;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.shortValue, resultSet.getShort("short_value"));

    assertMatch("shortValue = 10", assertion);
    assertNoMatch("shortValue = 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue = X"));

    assertNoMatch("shortValue != 10");
    assertMatch("shortValue != 11", assertion);
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue != X"));

    assertMatch("shortValue > 9", assertion);
    assertNoMatch("shortValue > 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue > X"));

    assertMatch("shortValue >= 9", assertion);
    assertMatch("shortValue >= 10", assertion);
    assertNoMatch("shortValue >= 11");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue >= X"));

    assertMatch("shortValue < 11", assertion);
    assertNoMatch("shortValue < 10");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue < X"));

    assertMatch("shortValue <= 11", assertion);
    assertMatch("shortValue <= 10", assertion);
    assertNoMatch("shortValue <= 9");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue <= X"));

    assertMatch("shortValue ∈ [9, 10, 11]", assertion);
    assertMatch("shortValue ∈ [10]", assertion);
    assertNoMatch("shortValue ∈ [9]");
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue ∈ X"));

    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue ≈ 11"));
    assertThrows(QueryException.class, () -> this.queryParser.parse("shortValue !≈ 10"));
  }

  @Test
  void shouldMatchString() throws SQLException
  {
    Target target = new Target();
    target.stringValue = "BB";

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet -> assertEquals(target.stringValue, resultSet.getString("string_value"));

    assertMatch("stringValue = BB", assertion);
    assertNoMatch("stringValue = AB");

    assertNoMatch("stringValue != BB");
    assertMatch("stringValue != AB", assertion);

    assertMatch("stringValue > AB", assertion);
    assertNoMatch("stringValue > BB");

    assertMatch("stringValue >= AB", assertion);
    assertMatch("stringValue >= BB", assertion);
    assertNoMatch("stringValue >= BC");

    assertMatch("stringValue < BC", assertion);
    assertNoMatch("stringValue < BB");

    assertMatch("stringValue <= BC", assertion);
    assertMatch("stringValue <= BB", assertion);
    assertNoMatch("stringValue <= AB");

    assertMatch("stringValue ∈ [AB, BB, BC]", assertion);
    assertMatch("stringValue ∈ [BB]", assertion);
    assertNoMatch("stringValue ∈ [AB]");

    assertMatch("stringValue ≈ B*", assertion);
    assertMatch("stringValue ≈ *B", assertion);
    assertMatch("stringValue ≈ *B*", assertion);
    assertNoMatch("stringValue ≈ *A*");
    assertNoMatch("stringValue ≈ *A*");
    assertNoMatch("stringValue ≈ *A*");

    assertNoMatch("stringValue !≈ B*");
    assertNoMatch("stringValue !≈ *B");
    assertNoMatch("stringValue !≈ *B*");
    assertMatch("stringValue !≈ *A*", assertion);
    assertMatch("stringValue !≈ *A*", assertion);
    assertMatch("stringValue !≈ *A*", assertion);
  }

  @Test
  void shouldMatchNull() throws SQLException
  {
    insertTargetWithNullValues();

    assertMatch("booleanValue = null");
    assertMatch("byteValue = null");
    assertMatch("charValue = null");
    assertMatch("dateValue = null");
    assertMatch("doubleValue = null");
    assertMatch("floatValue = null");
    assertMatch("intValue = null");
    assertMatch("localDateValue = null");
    assertMatch("localDateTimeValue = null");
    assertMatch("longValue = null");
    assertMatch("shortValue = null");
    assertMatch("stringValue = null");
  }

  @Test
  void shouldMatchUnknownField()
  {
    assertThrows(QueryParseException.class, () -> this.queryParser.parse("unknown = anything"));
  }

  @Test
  void shouldMatchAnd() throws SQLException
  {
    Target target = new Target();
    target.stringValue = "Testing";
    target.intValue = 123;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet ->
    {
      assertEquals(target.stringValue, resultSet.getString("string_value"));
      assertEquals(target.intValue, resultSet.getInt("int_value"));
    };

    assertMatch("stringValue = Testing && intValue = 123", assertion);
    assertNoMatch("stringValue = X && intValue = 123");
    assertNoMatch("stringValue = Testing && intValue = 0");
  }

  @Test
  void shouldMatchOr() throws SQLException
  {
    Target target = new Target();
    target.stringValue = "Testing";
    target.intValue = 123;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet ->
    {
      assertEquals(target.stringValue, resultSet.getString("string_value"));
      assertEquals(target.intValue, resultSet.getInt("int_value"));
    };

    assertMatch("stringValue = Testing || intValue = 123", assertion);
    assertMatch("stringValue = X || intValue = 123", assertion);
    assertMatch("stringValue = Testing || intValue = 0", assertion);
    assertNoMatch("stringValue = X || intValue = 0");
  }

  @Test
  void shouldMatchMultipleLogicalOperatorsWithParenthesis() throws SQLException
  {
    Target target = new Target();
    target.booleanValue = true;
    target.stringValue = "Testing";
    target.intValue = 123;

    insertTarget(target);

    Assertion<ResultSet, SQLException> assertion = resultSet ->
    {
      assertEquals(target.booleanValue, resultSet.getBoolean("boolean_value"));
      assertEquals(target.stringValue, resultSet.getString("string_value"));
      assertEquals(target.intValue, resultSet.getInt("int_value"));
    };

    assertMatch("booleanValue = true || (stringValue = X && intValue = 1)", assertion);
    assertMatch("booleanValue = false || (stringValue = Testing && intValue = 123)", assertion);
    assertNoMatch("booleanValue = false || (stringValue = X && intValue = 1)");
  }

  private void assertMatch(String query) throws SQLException
  {
    assertMatch(query, resultSet -> {});
  }

  private void assertMatch(String query, Assertion<ResultSet, SQLException> assertion) throws SQLException
  {
    JdbcPredicate predicate = this.queryParser.parse(query);

    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT + predicate.toSql()))
    {
      predicate.setValues(preparedStatement);

      try (ResultSet resultSet = preparedStatement.executeQuery())
      {
        assertTrue(resultSet.next());
        assertion.apply(resultSet);
        assertFalse(resultSet.next());
      }
    }
  }

  private void assertNoMatch(String query) throws SQLException
  {
    JdbcPredicate predicate = this.queryParser.parse(query);

    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_SELECT + predicate.toSql()))
    {
      predicate.setValues(preparedStatement);

      try (ResultSet resultSet = preparedStatement.executeQuery())
      {
        assertFalse(resultSet.next());
      }
    }
  }

  private static void createTable() throws Exception
  {
    try (Statement statement = connection.createStatement())
    {
      statement.execute(createTableSql());
    }
  }

  private static String createTableSql() throws IOException
  {
    try (InputStream in = ClassLoader.getSystemResourceAsStream(CREATE_TABLE_SCRIPT))
    {
      if (in == null) throw new IllegalStateException("Failed to find: " + CREATE_TABLE_SCRIPT);

      return new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8))
        .lines()
        .filter(line -> !line.startsWith("--"))
        .collect(Collectors.joining(SPACE));
    }
  }

  private static void insertTarget(Target target) throws SQLException
  {
    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT))
    {
      int index = 1;
      preparedStatement.setObject(index++, target.booleanValue, Types.BOOLEAN);
      preparedStatement.setObject(index++, target.byteValue, Types.TINYINT);
      preparedStatement.setObject(index++, target.charValue, Types.CHAR);
      preparedStatement.setTimestamp(index++, target.dateValue != null ? new Timestamp(target.dateValue.getTime()) : null);
      preparedStatement.setObject(index++, target.doubleValue, Types.DOUBLE);
      preparedStatement.setObject(index++, target.floatValue, Types.FLOAT);
      preparedStatement.setObject(index++, target.intValue, Types.INTEGER);
      preparedStatement.setDate(index++, target.localDateValue != null ? new java.sql.Date(target.localDateValue.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli()) : null);
      preparedStatement.setTimestamp(index++, target.localDateTimeValue != null ? new Timestamp(target.localDateTimeValue.toInstant(ZoneOffset.UTC).toEpochMilli()) : null);
      preparedStatement.setObject(index++, target.longValue, Types.BIGINT);
      preparedStatement.setObject(index++, target.shortValue, Types.SMALLINT);
      preparedStatement.setString(index, target.stringValue);
      preparedStatement.execute();
    }
  }

  private static void insertTargetWithNullValues() throws SQLException
  {
    try (PreparedStatement preparedStatement = connection.prepareStatement(SQL_INSERT))
    {
      int index = 1;
      preparedStatement.setNull(index++, Types.BOOLEAN);
      preparedStatement.setNull(index++, Types.TINYINT);
      preparedStatement.setNull(index++, Types.CHAR);
      preparedStatement.setNull(index++, Types.TIMESTAMP);
      preparedStatement.setNull(index++, Types.DOUBLE);
      preparedStatement.setNull(index++, Types.FLOAT);
      preparedStatement.setNull(index++, Types.INTEGER);
      preparedStatement.setNull(index++, Types.DATE);
      preparedStatement.setNull(index++, Types.TIMESTAMP);
      preparedStatement.setNull(index++, Types.BIGINT);
      preparedStatement.setNull(index++, Types.SMALLINT);
      preparedStatement.setNull(index, Types.VARCHAR);
      preparedStatement.execute();
    }
  }

  static class Target
  {
    Boolean booleanValue;
    Byte byteValue;
    Character charValue;
    Date dateValue;
    Double doubleValue;
    Float floatValue;
    Integer intValue;
    LocalDate localDateValue;
    LocalDateTime localDateTimeValue;
    Long longValue;
    Short shortValue;
    String stringValue;
  }

  @FunctionalInterface
  private interface Assertion<T, E extends Exception>
  {
    void apply(T t) throws E;
  }
}
