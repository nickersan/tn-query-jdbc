package com.tn.query.jdbc;

import static java.lang.String.format;
import static java.time.Month.DECEMBER;
import static java.time.Month.NOVEMBER;
import static java.time.Month.OCTOBER;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import com.tn.query.PredicateFactory;
import com.tn.query.QueryException;

class JdbcPredicateFactoryTest
{
  private static final Map<String, String> FIELD_MAPPINGS = new HashMap<>();

  static
  {
    FIELD_MAPPINGS.put("booleanValue", "boolean");
    FIELD_MAPPINGS.put("byteValue", "byte");
    FIELD_MAPPINGS.put("charValue", "char");
    FIELD_MAPPINGS.put("dateValue", "date");
    FIELD_MAPPINGS.put("doubleValue", "double");
    FIELD_MAPPINGS.put("floatValue", "float");
    FIELD_MAPPINGS.put("intValue", "int");
    FIELD_MAPPINGS.put("localDateValue", "localDate");
    FIELD_MAPPINGS.put("localDateTimeValue", "localDateTime");
    FIELD_MAPPINGS.put("longValue", "long");
    FIELD_MAPPINGS.put("shortValue", "short");
    FIELD_MAPPINGS.put("stringValue", "string");
  }

  private final PredicateFactory<JdbcPredicate> predicateFactory = new JdbcPredicateFactory(FIELD_MAPPINGS);

  @Test
  void shouldMatchBoolean() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("booleanValue", true),
      "boolean = true",
      "boolean = ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.notEqual("booleanValue", true),
      "NOT boolean = true",
      "NOT boolean = ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("booleanValue", true),
      "boolean > true",
      "boolean > ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("booleanValue", true),
      "boolean >= true",
      "boolean >= ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.lessThan("booleanValue", true),
      "boolean < true",
      "boolean < ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("booleanValue", true),
      "boolean <= true",
      "boolean <= ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.predicateFactory.in("booleanValue", List.of(true, false)),
      "boolean IN (true, false)",
      "boolean IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setBoolean(1, true);
        verify(preparedStatement).setBoolean(2, false);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("booleanValue", true));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("booleanValue", false));
  }

  @Test
  void shouldMatchByte() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("byteValue", (byte)1),
      "byte = 1",
      "byte = ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.notEqual("byteValue", (byte)1),
      "NOT byte = 1",
      "NOT byte = ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("byteValue", (byte)1),
      "byte > 1",
      "byte > ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("byteValue", (byte)1),
      "byte >= 1",
      "byte >= ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.lessThan("byteValue", (byte)1),
      "byte < 1",
      "byte < ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("byteValue", (byte)1),
      "byte <= 1",
      "byte <= ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.predicateFactory.in("byteValue", List.of((byte)1, (byte)2)),
      "byte IN (1, 2)",
      "byte IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setByte(1, (byte)1);
        verify(preparedStatement).setByte(2, (byte)2);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("byteValue", (byte)1));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("byteValue", (byte)2));
  }

  @Test
  void shouldMatchChar() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("charValue", 'b'),
      "char = b",
      "char = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.notEqual("charValue", 'b'),
      "NOT char = b",
      "NOT char = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.greaterThan("charValue", 'b'),
      "char > b",
      "char > ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("charValue", 'b'),
      "char >= b",
      "char >= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.lessThan("charValue", 'b'),
      "char < b",
      "char < ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("charValue", 'b'),
      "char <= b",
      "char <= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.predicateFactory.in("charValue", List.of('a', 'b', 'c')),
      "char IN (a, b, c)",
      "char IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "a");
        verify(preparedStatement).setString(2, "b");
        verify(preparedStatement).setString(3, "c");
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("charValue", 'b'));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("charValue", 'c'));
  }

  @Test
  void shouldMatchDate() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDate.of(2023, OCTOBER, 10).atStartOfDay(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.predicateFactory.equal("dateValue", timestamp),
      "date = " + timestamp,
      "date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.notEqual("dateValue", timestamp),
      "NOT date = " + timestamp,
      "NOT date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("dateValue", timestamp),
      "date > " + timestamp,
      "date > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("dateValue", timestamp),
      "date >= " + timestamp,
      "date >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.lessThan("dateValue", timestamp),
      "date < " + timestamp,
      "date < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("dateValue", timestamp),
      "date <= " + timestamp,
      "date <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDate.of(2024, NOVEMBER, 11).atStartOfDay(ZoneId.systemDefault()).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDate.of(2025, DECEMBER, 12).atStartOfDay(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.predicateFactory.in("dateValue", List.of(timestamp, timestamp2, timestamp3)),
      "date IN (" + String.join(", ", timestamp.toString(), timestamp2.toString(), timestamp3.toString()) + ")",
      "date IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("dateValue", timestamp));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("dateValue", timestamp));
  }

  @Test
  void shouldMatchDouble() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("doubleValue", 1.2),
      "double = 1.2",
      "double = ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.notEqual("doubleValue", 1.2),
      "NOT double = 1.2",
      "NOT double = ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("doubleValue", 1.2),
      "double > 1.2",
      "double > ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("doubleValue", 1.2),
      "double >= 1.2",
      "double >= ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.lessThan("doubleValue", 1.2),
      "double < 1.2",
      "double < ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("doubleValue", 1.2),
      "double <= 1.2",
      "double <= ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.predicateFactory.in("doubleValue", List.of(1.2, 2.3)),
      "double IN (1.2, 2.3)",
      "double IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setDouble(1, 1.2);
        verify(preparedStatement).setDouble(2, 2.3);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("doubleValue", 1.2));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("doubleValue", 1.1));
  }

  @Test
  void shouldMatchFloat() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("floatValue", 1.2f),
      "float = 1.2",
      "float = ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.notEqual("floatValue", 1.2f),
      "NOT float = 1.2",
      "NOT float = ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("floatValue", 1.2f),
      "float > 1.2",
      "float > ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("floatValue", 1.2f),
      "float >= 1.2",
      "float >= ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.lessThan("floatValue", 1.2f),
      "float < 1.2",
      "float < ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("floatValue", 1.2f),
      "float <= 1.2",
      "float <= ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.predicateFactory.in("floatValue", List.of(1.2f, 2.3f)),
      "float IN (1.2, 2.3)",
      "float IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setFloat(1, 1.2f);
        verify(preparedStatement).setFloat(2, 2.3f);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("floatValue", 1.2f));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("floatValue", 1.1f));
  }

  @Test
  void shouldMatchInt() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("intValue", 1),
      "int = 1",
      "int = ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.notEqual("intValue", 1),
      "NOT int = 1",
      "NOT int = ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("intValue", 1),
      "int > 1",
      "int > ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("intValue", 1),
      "int >= 1",
      "int >= ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.lessThan("intValue", 1),
      "int < 1",
      "int < ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("intValue", 1),
      "int <= 1",
      "int <= ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.predicateFactory.in("intValue", List.of(1, 2)),
      "int IN (1, 2)",
      "int IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setInt(1, 1);
        verify(preparedStatement).setInt(2, 2);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("intValue", 2));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("intValue", 1));
  }

  @Test
  void shouldMatchLocalDate() throws Exception
  {
    LocalDate localDate = LocalDate.of(2023, OCTOBER, 10);
    Date date = Date.valueOf(localDate);

    assertPredicate(
      this.predicateFactory.equal("localDateValue", localDate),
      "localDate = " + localDate,
      "localDate = ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.predicateFactory.notEqual("localDateValue", localDate),
      "NOT localDate = " + localDate,
      "NOT localDate = ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("localDateValue", localDate),
      "localDate > " + localDate,
      "localDate > ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("localDateValue", localDate),
      "localDate >= " + localDate,
      "localDate >= ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.predicateFactory.lessThan("localDateValue", localDate),
      "localDate < " + localDate,
      "localDate < ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("localDateValue", localDate),
      "localDate <= " + localDate,
      "localDate <= ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    LocalDate localDate2 = LocalDate.of(2024, NOVEMBER, 11);
    Date date2 = new Date(localDate2.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    LocalDate localDate3 = LocalDate.of(2025, DECEMBER, 12);
    Date date3 = new Date(localDate3.atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    assertPredicate(
      this.predicateFactory.in("localDateValue", List.of(localDate, localDate2, localDate3)),
      "localDate IN (" + String.join(", ", localDate.toString(), localDate2.toString(), localDate3.toString()) + ")",
      "localDate IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setDate(1, date);
        verify(preparedStatement).setDate(2, date2);
        verify(preparedStatement).setDate(3, date3);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("localDateValue", localDate));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("localDateValue", localDate));
  }

  @Test
  void shouldMatchLocalDateTime() throws Exception
  {
    LocalDateTime localDateTime = LocalDateTime.now();
    Timestamp timestamp = Timestamp.valueOf(localDateTime);

    assertPredicate(
      this.predicateFactory.equal("localDateValue", localDateTime),
      format("localDate = %s", localDateTime),
      "localDate = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.notEqual("localDateValue", localDateTime),
      format("NOT localDate = %s", localDateTime),
      "NOT localDate = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("localDateValue", localDateTime),
      format("localDate > %s", localDateTime),
      "localDate > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("localDateValue", localDateTime),
      format("localDate >= %s", localDateTime),
      "localDate >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.lessThan("localDateValue", localDateTime),
      format("localDate < %s", localDateTime),
      "localDate < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("localDateValue", localDateTime),
      format("localDate <= %s", localDateTime),
      "localDate <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    LocalDateTime localDateTime2 = localDateTime.plusHours(1);
    Timestamp timestamp2 = Timestamp.valueOf(localDateTime2);

    LocalDateTime localDateTime3 = localDateTime.plusHours(2);
    Timestamp timestamp3 = Timestamp.valueOf(localDateTime3);

    assertPredicate(
      this.predicateFactory.in("localDateValue", List.of(localDateTime, localDateTime2, localDateTime3)),
      format("localDate IN (%s, %s, %s)", localDateTime, localDateTime2, localDateTime3),
      "localDate IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("localDateValue", localDateTime));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("localDateValue", localDateTime));
  }

  @Test
  void shouldMatchLong() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("longValue", 1L),
      "long = 1",
      "long = ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.notEqual("longValue", 1L),
      "NOT long = 1",
      "NOT long = ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("longValue", 1L),
      "long > 1",
      "long > ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("longValue", 1L),
      "long >= 1",
      "long >= ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.lessThan("longValue", 1L),
      "long < 1",
      "long < ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("longValue", 1L),
      "long <= 1",
      "long <= ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.predicateFactory.in("longValue", List.of(1L, 2L)),
      "long IN (1, 2)",
      "long IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).setLong(2, 2L);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("longValue", 1L));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("longValue", 1L));
  }

  @Test
  void shouldMatchShort() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("shortValue", (short)1),
      "short = 1",
      "short = ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.notEqual("shortValue", (short)1),
      "NOT short = 1",
      "NOT short = ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.greaterThan("shortValue", (short)1),
      "short > 1",
      "short > ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("shortValue", (short)1),
      "short >= 1",
      "short >= ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.lessThan("shortValue", (short)1),
      "short < 1",
      "short < ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("shortValue", (short)1),
      "short <= 1",
      "short <= ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.predicateFactory.in("shortValue", List.of((short)1, (short)2)),
      "short IN (1, 2)",
      "short IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setShort(1, (short)1);
        verify(preparedStatement).setShort(2, (short)2);
      }
    );

    assertThrows(QueryException.class, () -> this.predicateFactory.like("shortValue", (short)2));
    assertThrows(QueryException.class, () -> this.predicateFactory.notLike("shortValue", (short)1));
  }

  @Test
  void shouldMatchString() throws Exception
  {
    assertPredicate(
      this.predicateFactory.equal("stringValue", "ABC"),
      "string = ABC",
      "string = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.notEqual("stringValue", "ABC"),
      "NOT string = ABC",
      "NOT string = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.greaterThan("stringValue", "ABC"),
      "string > ABC",
      "string > ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.greaterThanOrEqual("stringValue", "ABC"),
      "string >= ABC",
      "string >= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.lessThan("stringValue", "ABC"),
      "string < ABC",
      "string < ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.lessThanOrEqual("stringValue", "ABC"),
      "string <= ABC",
      "string <= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC")
    );

    assertPredicate(
      this.predicateFactory.like("stringValue", "ABC*"),
      "string LIKE ABC%",
      "string LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC%")
    );

    assertPredicate(
      this.predicateFactory.like("stringValue", "*ABC"),
      "string LIKE %ABC",
      "string LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "%ABC")
    );

    assertPredicate(
      this.predicateFactory.like("stringValue", "*ABC*"),
      "string LIKE %ABC%",
      "string LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "%ABC%")
    );

    assertPredicate(
      this.predicateFactory.notLike("stringValue", "ABC*"),
      "string NOT LIKE ABC%",
      "string NOT LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "ABC%")
    );

    assertPredicate(
      this.predicateFactory.notLike("stringValue", "*ABC"),
      "string NOT LIKE %ABC",
      "string NOT LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "%ABC")
    );

    assertPredicate(
      this.predicateFactory.notLike("stringValue", "*ABC*"),
      "string NOT LIKE %ABC%",
      "string NOT LIKE ?",
      preparedStatement -> verify(preparedStatement).setString(1, "%ABC%")
    );

    assertPredicate(
      this.predicateFactory.in("stringValue", List.of("ABC", "DEF")),
      "string IN (ABC, DEF)",
      "string IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "ABC");
        verify(preparedStatement).setString(2, "DEF");
      }
    );
  }

  @Test
  void shouldMatchAnd() throws Exception
  {
    assertPredicate(
      this.predicateFactory.and(
        this.predicateFactory.equal("stringValue", "Testing"),
        this.predicateFactory.equal("intValue", 123)
      ),
      "string = Testing AND int = 123",
      "string = ? AND int = ?",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "Testing");
        verify(preparedStatement).setInt(2, 123);
      }
    );
  }

  @Test
  void shouldMatchOr() throws Exception
  {
    assertPredicate(
      this.predicateFactory.or(
        this.predicateFactory.equal("stringValue", "Testing"),
        this.predicateFactory.equal("intValue", 123)
      ),
      "string = Testing OR int = 123",
      "string = ? OR int = ?",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "Testing");
        verify(preparedStatement).setInt(2, 123);
      }
    );
  }

  @Test
  void shouldMatchMultipleLogicalOperatorsWithParenthesis() throws Exception
  {
    assertPredicate(
      this.predicateFactory.and(
        this.predicateFactory.equal("stringValue", "A"),
        this.predicateFactory.parenthesis(
          this.predicateFactory.or(
            this.predicateFactory.notEqual("stringValue", "B"),
            this.predicateFactory.parenthesis(
              this.predicateFactory.and(
                this.predicateFactory.greaterThan("intValue", 1),
                this.predicateFactory.lessThanOrEqual("intValue", 10)
              )
            )
          )
        )
      ),
      "string = A AND (NOT string = B OR (int > 1 AND int <= 10))",
      "string = ? AND (NOT string = ? OR (int > ? AND int <= ?))",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "A");
        verify(preparedStatement).setString(2, "B");
        verify(preparedStatement).setInt(3, 1);
        verify(preparedStatement).setInt(4, 10);
      }
    );
  }

  private void assertPredicate(JdbcPredicate predicate, String expectedString, String expectedSql, Assertion<PreparedStatement> assertion) throws Exception
  {
    assertEquals(expectedString, predicate.toString());
    assertEquals(expectedSql, predicate.toSql());

    PreparedStatement preparedStatement = mock(PreparedStatement.class);
    predicate.setValues(preparedStatement);

    assertion.run(preparedStatement);
  }

  @FunctionalInterface
  private interface Assertion<T>
  {
    void run(T value) throws Exception;
  }
}
