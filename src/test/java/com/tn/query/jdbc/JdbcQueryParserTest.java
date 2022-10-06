package com.tn.query.jdbc;

import static java.time.Month.NOVEMBER;
import static java.time.Month.OCTOBER;
import static java.time.Month.SEPTEMBER;

import static org.junit.jupiter.api.Assertions.assertEquals;
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

import com.tn.query.Mapper;
import com.tn.query.QueryParser;

class JdbcQueryParserTest
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

  private final QueryParser<JdbcPredicate> queryParser = new JdbcQueryParser(
    FIELD_MAPPINGS,
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

  @Test
  void testParseBoolean() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("booleanValue = true"),
      "boolean = ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue != true"),
      "NOT boolean = ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue > true"),
      "boolean > ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue >= true"),
      "boolean >= ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue < true"),
      "boolean < ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue <= true"),
      "boolean <= ?",
      preparedStatement -> verify(preparedStatement).setBoolean(1, true)
    );

    assertPredicate(
      this.queryParser.parse("booleanValue ∈ [true, false]"),
      "boolean IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setBoolean(1, true);
        verify(preparedStatement).setBoolean(2, false);
      }
    );
  }

  @Test
  void testParseByte() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("byteValue = 1"),
      "byte = ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue != 1"),
      "NOT byte = ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue > 1"),
      "byte > ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue >= 1"),
      "byte >= ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue < 1"),
      "byte < ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue <= 1"),
      "byte <= ?",
      preparedStatement -> verify(preparedStatement).setByte(1, (byte)1)
    );

    assertPredicate(
      this.queryParser.parse("byteValue ∈ [1, 2]"),
      "byte IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setByte(1, (byte)1);
        verify(preparedStatement).setByte(2, (byte)2);
      }
    );
  }

  @Test
  void testParseChar() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("charValue = b"),
      "char = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue != b"),
      "NOT char = ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue > b"),
      "char > ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue >= b"),
      "char >= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue < b"),
      "char < ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue <= b"),
      "char <= ?",
      preparedStatement -> verify(preparedStatement).setString(1, "b")
    );

    assertPredicate(
      this.queryParser.parse("charValue ∈ [a, b, c]"),
      "char IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setString(1, "a");
        verify(preparedStatement).setString(2, "b");
        verify(preparedStatement).setString(3, "c");
      }
    );
  }

  @Test
  void testParseDate() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDate.of(2021, SEPTEMBER, 25).atStartOfDay(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue = 2021-09-25"),
      "date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue != 2021-09-25"),
      "NOT date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue > 2021-09-25"),
      "date > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue >= 2021-09-25"),
      "date >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue < 2021-09-25"),
      "date < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue <= 2021-09-25"),
      "date <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDate.of(2022, OCTOBER, 26).atStartOfDay(ZoneId.systemDefault()).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDate.of(2023, NOVEMBER, 27).atStartOfDay(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue ∈ [2021-09-25, 2022-10-26, 2023-11-27]"),
      "date IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseDateTimeWithMinutes() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue = 2021-09-25T10:15"),
      "date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue != 2021-09-25T10:15"),
      "NOT date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue > 2021-09-25T10:15"),
      "date > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue >= 2021-09-25T10:15"),
      "date >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue < 2021-09-25T10:15"),
      "date < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue <= 2021-09-25T10:15"),
      "date <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16).atZone(ZoneId.systemDefault()).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue ∈ [2021-09-25T10:15, 2022-10-26T11:16, 2023-11-27T12:17]"),
      "date IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseDateTimeWithSeconds() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15, 16).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue = 2021-09-25T10:15:16"),
      "date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue != 2021-09-25T10:15:16"),
      "NOT date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue > 2021-09-25T10:15:16"),
      "date > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue >= 2021-09-25T10:15:16"),
      "date >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue < 2021-09-25T10:15:16"),
      "date < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue <= 2021-09-25T10:15:16"),
      "date <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16, 17).atZone(ZoneId.systemDefault()).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17, 18).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue ∈ [2021-09-25T10:15:16, 2022-10-26T11:16:17, 2023-11-27T12:17:18]"),
      "date IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseDateTimeWithMilliseconds() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15, 16, 17_000_000).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue = 2021-09-25T10:15:16.17"),
      "date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue != 2021-09-25T10:15:16.17"),
      "NOT date = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue > 2021-09-25T10:15:16.17"),
      "date > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue >= 2021-09-25T10:15:16.17"),
      "date >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue < 2021-09-25T10:15:16.17"),
      "date < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("dateValue <= 2021-09-25T10:15:16.17"),
      "date <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16, 17, 18_000_000).atZone(ZoneId.systemDefault()).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17, 18, 19_000_000).atZone(ZoneId.systemDefault()).toInstant());

    assertPredicate(
      this.queryParser.parse("dateValue ∈ [2021-09-25T10:15:16.17, 2022-10-26T11:16:17.18, 2023-11-27T12:17:18.19]"),
      "date IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseDouble() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("doubleValue = 1.2"),
      "double = ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue != 1.2"),
      "NOT double = ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue > 1.2"),
      "double > ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue >= 1.2"),
      "double >= ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue < 1.2"),
      "double < ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue <= 1.2"),
      "double <= ?",
      preparedStatement -> verify(preparedStatement).setDouble(1, 1.2)
    );

    assertPredicate(
      this.queryParser.parse("doubleValue ∈ [1.2, 2.3]"),
      "double IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setDouble(1, 1.2);
        verify(preparedStatement).setDouble(2, 2.3);
      }
    );
  }

  @Test
  void testParseFloat() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("floatValue = 1.2"),
      "float = ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue != 1.2"),
      "NOT float = ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue > 1.2"),
      "float > ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue >= 1.2"),
      "float >= ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue < 1.2"),
      "float < ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue <= 1.2"),
      "float <= ?",
      preparedStatement -> verify(preparedStatement).setFloat(1, 1.2f)
    );

    assertPredicate(
      this.queryParser.parse("floatValue ∈ [1.2, 2.3]"),
      "float IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setFloat(1, 1.2f);
        verify(preparedStatement).setFloat(2, 2.3f);
      }
    );
  }

  @Test
  void testParseInt() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("intValue = 1"),
      "int = ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue != 1"),
      "NOT int = ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue > 1"),
      "int > ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue >= 1"),
      "int >= ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue < 1"),
      "int < ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue <= 1"),
      "int <= ?",
      preparedStatement -> verify(preparedStatement).setInt(1, 1)
    );

    assertPredicate(
      this.queryParser.parse("intValue ∈ [1, 2]"),
      "int IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setInt(1, 1);
        verify(preparedStatement).setInt(2, 2);
      }
    );
  }

  @Test
  void testParseLocalDate() throws Exception
  {
    Date date = new Date(LocalDate.of(2021, SEPTEMBER, 25).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    assertPredicate(
      this.queryParser.parse("localDateValue = 2021-09-25"),
      "localDate = ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.queryParser.parse("localDateValue != 2021-09-25"),
      "NOT localDate = ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.queryParser.parse("localDateValue > 2021-09-25"),
      "localDate > ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.queryParser.parse("localDateValue >= 2021-09-25"),
      "localDate >= ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.queryParser.parse("localDateValue < 2021-09-25"),
      "localDate < ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    assertPredicate(
      this.queryParser.parse("localDateValue <= 2021-09-25"),
      "localDate <= ?",
      preparedStatement -> verify(preparedStatement).setDate(1, date)
    );

    Date date2 = new Date(LocalDate.of(2022, OCTOBER, 26).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());
    Date date3 = new Date(LocalDate.of(2023, NOVEMBER, 27).atStartOfDay(ZoneOffset.UTC).toInstant().toEpochMilli());

    assertPredicate(
      this.queryParser.parse("localDateValue ∈ [2021-09-25, 2022-10-26, 2023-11-27]"),
      "localDate IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setDate(1, date);
        verify(preparedStatement).setDate(2, date2);
        verify(preparedStatement).setDate(3, date3);
      }
    );
  }

  @Test
  void testParseLocalDateTimeWithMinutes() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15).toInstant(ZoneOffset.UTC));

    assertPredicate(
      this.queryParser.parse("localDateTimeValue = 2021-09-25T10:15"),
      "localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue != 2021-09-25T10:15"),
      "NOT localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue > 2021-09-25T10:15"),
      "localDateTime > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue >= 2021-09-25T10:15"),
      "localDateTime >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue < 2021-09-25T10:15"),
      "localDateTime < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue <= 2021-09-25T10:15"),
      "localDateTime <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16).atZone(ZoneOffset.UTC).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17).atZone(ZoneOffset.UTC).toInstant());

    assertPredicate(
      this.queryParser.parse("localDateTimeValue ∈ [2021-09-25T10:15, 2022-10-26T11:16, 2023-11-27T12:17]"),
      "localDateTime IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseLocalDateTimeWithSeconds() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15, 16).toInstant(ZoneOffset.UTC));

    assertPredicate(
      this.queryParser.parse("localDateTimeValue = 2021-09-25T10:15:16"),
      "localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue != 2021-09-25T10:15:16"),
      "NOT localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue > 2021-09-25T10:15:16"),
      "localDateTime > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue >= 2021-09-25T10:15:16"),
      "localDateTime >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue < 2021-09-25T10:15:16"),
      "localDateTime < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue <= 2021-09-25T10:15:16"),
      "localDateTime <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16, 17).atZone(ZoneOffset.UTC).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17, 18).atZone(ZoneOffset.UTC).toInstant());

    assertPredicate(
      this.queryParser.parse("localDateTimeValue ∈ [2021-09-25T10:15:16, 2022-10-26T11:16:17, 2023-11-27T12:17:18]"),
      "localDateTime IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseLocalDateTimeWithMilliseconds() throws Exception
  {
    Timestamp timestamp = Timestamp.from(LocalDateTime.of(2021, SEPTEMBER, 25, 10, 15, 16, 170_000_000).atZone(ZoneOffset.UTC).toInstant());

    assertPredicate(
      this.queryParser.parse("localDateTimeValue = 2021-09-25T10:15:16.17"),
      "localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue != 2021-09-25T10:15:16.17"),
      "NOT localDateTime = ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue > 2021-09-25T10:15:16.17"),
      "localDateTime > ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue >= 2021-09-25T10:15:16.17"),
      "localDateTime >= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue < 2021-09-25T10:15:16.17"),
      "localDateTime < ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    assertPredicate(
      this.queryParser.parse("localDateTimeValue <= 2021-09-25T10:15:16.17"),
      "localDateTime <= ?",
      preparedStatement -> verify(preparedStatement).setTimestamp(1, timestamp)
    );

    Timestamp timestamp2 = Timestamp.from(LocalDateTime.of(2022, OCTOBER, 26, 11, 16, 17, 180_000_000).atZone(ZoneOffset.UTC).toInstant());
    Timestamp timestamp3 = Timestamp.from(LocalDateTime.of(2023, NOVEMBER, 27, 12, 17, 18, 190_000_000).atZone(ZoneOffset.UTC).toInstant());

    assertPredicate(
      this.queryParser.parse("localDateTimeValue ∈ [2021-09-25T10:15:16.17, 2022-10-26T11:16:17.18, 2023-11-27T12:17:18.19]"),
      "localDateTime IN (?, ?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setTimestamp(1, timestamp);
        verify(preparedStatement).setTimestamp(2, timestamp2);
        verify(preparedStatement).setTimestamp(3, timestamp3);
      }
    );
  }

  @Test
  void testParseLong() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("longValue = 1"),
      "long = ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue != 1"),
      "NOT long = ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue > 1"),
      "long > ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue >= 1"),
      "long >= ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue < 1"),
      "long < ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue <= 1"),
      "long <= ?",
      preparedStatement -> verify(preparedStatement).setLong(1, 1L)
    );

    assertPredicate(
      this.queryParser.parse("longValue ∈ [1, 2]"),
      "long IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setLong(1, 1L);
        verify(preparedStatement).setLong(2, 2L);
      }
    );
  }

  @Test
  void testParseShortEqual() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("shortValue = 1"),
      "short = ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue != 1"),
      "NOT short = ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue > 1"),
      "short > ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue >= 1"),
      "short >= ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue < 1"),
      "short < ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue <= 1"),
      "short <= ?",
      preparedStatement -> verify(preparedStatement).setShort(1, (short)1)
    );

    assertPredicate(
      this.queryParser.parse("shortValue ∈ [1, 2]"),
      "short IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setShort(1, (short)1);
        verify(preparedStatement).setShort(2, (short)2);
      }
    );
  }

  @Test
  void testParseStringEqual() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("stringValue = ABC"),
      "string = ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue != ABC"),
      "NOT string = ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue > ABC"),
      "string > ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue >= ABC"),
      "string >= ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue < ABC"),
      "string < ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue <= ABC"),
      "string <= ?",
      preparedStatement -> verify(preparedStatement).setObject(1, "ABC")
    );

    assertPredicate(
      this.queryParser.parse("stringValue ∈ [ABC, DEF]"),
      "string IN (?, ?)",
      preparedStatement -> {
        verify(preparedStatement).setObject(1, "ABC");
        verify(preparedStatement).setObject(2, "DEF");
      }
    );
  }

  @Test
  void testParseAnd() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("stringValue = Testing && intValue = 123"),
      "string = ? AND int = ?",
      preparedStatement -> {
        verify(preparedStatement).setObject(1, "Testing");
        verify(preparedStatement).setInt(2, 123);
      }
    );
  }

  @Test
  void testParseOr() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("stringValue = Testing || intValue = 123"),
      "string = ? OR int = ?",
      preparedStatement -> {
        verify(preparedStatement).setObject(1, "Testing");
        verify(preparedStatement).setInt(2, 123);
      }
    );
  }

  @Test
  void testParseMultipleLogicalOperatorsWithParenthesis() throws Exception
  {
    assertPredicate(
      this.queryParser.parse("stringValue = A && (stringValue != B || (intValue > 1 && intValue <= 10))"),
      "string = ? AND (NOT string = ? OR (int > ? AND int <= ?))",
      preparedStatement -> {
        verify(preparedStatement).setObject(1, "A");
        verify(preparedStatement).setObject(2, "B");
        verify(preparedStatement).setInt(3, 1);
        verify(preparedStatement).setInt(4, 10);
      }
    );
  }

  private void assertPredicate(JdbcPredicate predicate, String expectedSql, Assertion<PreparedStatement> assertion) throws Exception
  {
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
