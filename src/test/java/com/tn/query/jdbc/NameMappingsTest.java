package com.tn.query.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class NameMappingsTest
{
  @Test
  void testForFields()
  {
    assertEquals(
      Map.of("oneValue", "one_value", "twoValue", "two_value"),
      NameMappings.forFields(Subject.class)
    );
  }

  @Test
  void testForFieldsWithIgnored()
  {
    assertEquals(
      Map.of("oneValue", "one_value"),
      NameMappings.forFields(Subject.class, List.of("twoValue"))
    );
  }

  @Test
  void testForFieldsWithOverride()
  {
    assertEquals(
      Map.of("oneValue", "uno_value", "twoValue", "two_value"),
      NameMappings.forFields(Subject.class, Map.of("oneValue", "uno_value"))
    );
  }

  @Test
  void testForFieldsWithIgnoredAndOverride()
  {
    assertEquals(
      Map.of("oneValue", "uno_value"),
      NameMappings.forFields(Subject.class, List.of("twoValue"), Map.of("oneValue", "uno_value"))
    );
  }

  @SuppressWarnings("unused")
  private static class Subject
  {
    private String oneValue;
    private String twoValue;
  }
}
