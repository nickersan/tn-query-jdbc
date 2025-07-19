# tn-query-jdbc

TN Query JDBC provides an implementation of [tn-query](https://github.com/nickersan/tn-query#readme) that builds JDBC (SQL) predicates that can be used with 
`java.sql.PreparedStatement`s.

## Usage

For each table you want to query create an instance of `com.tn.query.jdbc.JdbcPredicateFactory` passing:

1. any mappings between the names used in the query string and the column names in the table being queried.
2. the `com.tn.query.Mapper`s used to convert the string value from the query string, to a value passed to the `java.sql.PreparedStatement`s.

For example, given a table:
```
person(
  person_id  NUMBER,
  first_name VARCHAR,
  last_name  VARCHAR,
  sex        VARCHAR
)
```

An instance of `com.tn.query.jdbc.JdbcPredicateFactory` could be created as follows:
```java
QueryParser<JdbcPredicate> queryParser = new DefaultQueryParser(
  new JdbcPredicateFactory(
    Map.of(
      "id", "person_id",
      "firstName", "first_name",
      "lastName", "last_name",
    )
  ),
  List.of(
    Mapper.toInt("id"),
    Mapper.toEnum("sex", Sex.class)
  )
);
```

And would be used:
```java
JdbcPredicate predicate = this.queryParser.parse("((firstName = John && sex = m) || (firstName = Jane && sex = f)) && lastName = Smith");
PreparedStatement preparedStatement = connection.prepareStatement(predicate.toSql());
predicate.setValues(preparedStatement);
```

Note: when no mapper is provided, the value in the query string will be treated as a `java.lang.String`.

## Build

Typically, the command `mvn clean install` is used, which builds and packages, runs unit and integration tests and installs the artifacts into the local
[Maven](https://maven.apache.org/) repository.

See [tn-parent](..\tn-parent\README.md) for more details regarding the build.