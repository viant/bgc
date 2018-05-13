CREATE OR REPLACE TABLE travelers1 (
  id INT64,
  name STRING,
  lastVisitTime TIMESTAMP,
  visitedCities ARRAY<STRUCT<
    visits INT64,
    city STRING
  >>,
  achievements ARRAY<STRING>,
  mostLikedCity STRUCT<
  visits INT64,
  city STRING,
  souvenirs ARRAY<STRING>
  >
);




CREATE OR REPLACE TABLE travelers2 AS SELECT * FROM travelers1;

CREATE OR REPLACE TABLE travelers3 AS SELECT * FROM travelers1;

CREATE OR REPLACE TABLE travelers4 AS SELECT * FROM travelers1;

CREATE OR REPLACE TABLE travelers5 AS SELECT * FROM travelers1;
