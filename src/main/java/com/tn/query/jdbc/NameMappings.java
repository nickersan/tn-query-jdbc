package com.tn.query.jdbc;

import static java.lang.Character.isUpperCase;
import static java.lang.Character.toLowerCase;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.stream.Collectors.toUnmodifiableSet;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;

public class NameMappings
{
  private static final String JACOCO_FIELD = "$jacocoData";
  private static final String UNDERSCORE = "_";

  private NameMappings() {}

  public static Map<String, String> forFields(Class<?> subject)
  {
    return forFields(subject, emptyList(), emptyMap());
  }

  public static Map<String, String> forFields(Class<?> subject, Collection<String> ignored)
  {
    return forFields(subject, ignored, emptyMap());
  }

  public static Map<String, String> forFields(Class<?> subject, Map<String, String> overrides)
  {
    return forFields(subject, emptyList(), overrides);
  }

  public static Map<String, String> forFields(Class<?> subject, Collection<String> ignored, Map<String, String> overrides)
  {
    Map<String, String> mappings = new HashMap<>(overrides);
    fieldNames(subject, ignored).forEach(fieldName -> mappings.putIfAbsent(fieldName, override(overrides, fieldName)));

    return unmodifiableMap(mappings);
  }

  private static String override(Map<String, String> overrides, String fieldName)
  {
    String override = overrides.get(fieldName);
    return override != null ? override : columnName(fieldName);
  }

  private static String columnName(String fieldName)
  {
    StringBuilder columnName = new StringBuilder();

    for (int i = 0; i < fieldName.length(); i++)
    {
      char c = fieldName.charAt(i);
      if (isUpperCase(c)) columnName.append(UNDERSCORE).append(toLowerCase(c));
      else columnName.append(c);
    }

    return columnName.toString();
  }

  private static Collection<String> fieldNames(Class<?> subject, Collection<String> ignored)
  {
    return Stream.of(subject.getDeclaredFields())
      .map(Field::getName)
      .filter(fieldName -> !JACOCO_FIELD.equals(fieldName) && !ignored.contains(fieldName))
      .collect(toUnmodifiableSet());
  }
}
