[//]: # (title: Import SQL Metadata as a Schema in Gradle Project)

<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.Schemas-->

Each SQL database contains the metadata for all the tables. 
This metadata could be used for the schema generation.

**NOTE:** Visit this [page](readSqlDatabases.md) to see how to set up all Gradle dependencies for your project.

### With `@file:ImportDataSchema`

To generate schema for existing SQL table,
you need to define a few parameters to establish JDBC connection:
URL, username, and password.

Also, the `tableName` parameter could be specified.

You should also specify the name of the generated Kotlin class 
as the first parameter of the annotation `@file:ImportDataSchema`.

```kotlin
@file:ImportDataSchema(
    "ActorSchema",
    URL,
    jdbcOptions = JdbcOptions(USER_NAME, PASSWORD, tableName = TABLE_NAME)
)

package databases

import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
```

```kotlin
const val URL = "jdbc:mariadb://localhost:3306/imdb"

const val USER_NAME = "root"

const val PASSWORD = "pass"

const val TABLE_NAME = "actors"
```
To generate schema for the result of an SQL query,
you need to define the SQL query itself
and the same parameters to establish connection with the database.

You should also specify the name of the generated Kotlin class
as a first parameter of annotation `@file:ImportDataSchema`.

```kotlin
@file:ImportDataSchema(
    "TarantinoFilmSchema",
    URL,
    jdbcOptions = JdbcOptions(USER_NAME, PASSWORD, sqlQuery = TARANTINO_FILMS_SQL_QUERY)
)

package databases

import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
```

```kotlin
const val URL = "jdbc:mariadb://localhost:3306/imdb"

const val USER_NAME = "root"

const val PASSWORD = "pass"

const val TARANTINO_FILMS_SQL_QUERY = """
    SELECT name, year, rank,
    GROUP_CONCAT (genre) as "genres"
    FROM movies JOIN movies_directors ON movie_id = movies.id
    JOIN directors ON directors.id=director_id LEFT JOIN movies_genres ON movies.id = movies_genres.movie_id
    WHERE directors.first_name = "Quentin" AND directors.last_name = "Tarantino"
    GROUP BY name, year, rank
    ORDER BY year
    """
```

### With Gradle Task 

To generate a schema for an existing SQL table,
you need to define a few parameters to establish a JDBC connection:
URL (passing to `data` field), username, and password.

Also, the `tableName` parameter should be specified to convert the data from the table with that name to the dataframe.

```kotlin
dataframes {
    schema {
        data = "jdbc:mariadb://localhost:3306/imdb"
        name = "org.example.imdb.Actors"
        jdbcOptions {
            user = "root"
            password = "pass" 
            tableName = "actors"
        }
    }
}
```

To generate a schema for the result of an SQL query,
you need to define the same parameters as before together with the SQL query to establish connection.


```kotlin
dataframes {
    schema {
        data = "jdbc:mariadb://localhost:3306/imdb"
        name = "org.example.imdb.TarantinoFilms"
        jdbcOptions {
            user = "root" 
            password = "pass"
            sqlQuery = """
                SELECT name, year, rank,
                GROUP_CONCAT (genre) as "genres"
                FROM movies JOIN movies_directors ON movie_id = movies.id
                JOIN directors ON directors.id=director_id LEFT JOIN movies_genres ON movies.id = movies_genres.movie_id
                WHERE directors.first_name = "Quentin" AND directors.last_name = "Tarantino"
                GROUP BY name, year, rank
                ORDER BY year
                """
        }
    }
}
```

After importing the data schema, you can start to import any data from SQL table or as a result of an SQL query
you like using the generated schemas.

Now you will have a correctly typed [`DataFrame`](DataFrame.md)!

If you experience any issues with the SQL databases support (since there are many edge-cases when converting
SQL types from different databases to Kotlin types), please open an issue on
the [GitHub repo](https://github.com/Kotlin/dataframe/issues), specifying the database and the problem.
