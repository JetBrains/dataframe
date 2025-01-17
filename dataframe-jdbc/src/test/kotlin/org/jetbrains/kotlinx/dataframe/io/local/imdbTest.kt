//package org.jetbrains.kotlinx.dataframe.io.local
//
//import io.kotest.matchers.shouldBe
//import org.intellij.lang.annotations.Language
//import org.jetbrains.kotlinx.dataframe.DataFrame
//import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
//import org.jetbrains.kotlinx.dataframe.api.cast
//import org.jetbrains.kotlinx.dataframe.api.filter
//import org.jetbrains.kotlinx.dataframe.io.getSchemaForSqlQuery
//import org.jetbrains.kotlinx.dataframe.io.getSchemaForSqlTable
//import org.jetbrains.kotlinx.dataframe.io.readSqlQuery
//import org.jetbrains.kotlinx.dataframe.io.readSqlTable
//import org.junit.Ignore
//import org.junit.Test
//import java.sql.DriverManager
//import java.util.Properties
//import kotlin.reflect.typeOf
//
//private const val URL = "jdbc:mariadb://localhost:3307/imdb"
//private const val URL2 = "jdbc:mariadb://localhost:3307"
//private const val USER_NAME = "root"
//private const val PASSWORD = "pass"
//
//@DataSchema
//interface ActorKDF {
//    val id: Int
//    val firstName: String?
//    val lastName: String?
//    val gender: String?
//}
//
//@DataSchema
//interface RankedMoviesWithGenres {
//    val name: String?
//    val year: Int?
//    val rank: Float?
//    val genres: String?
//}
//
//@Ignore
//class ImdbTestTest {
//    @Test
//    fun `read table`() {
//        val props = Properties()
//        props.setProperty("user", USER_NAME)
//        props.setProperty("password", PASSWORD)
//
//        // generate kdf schemas by database metadata (as interfaces or extensions)
//        // for gradle or as classes under the hood in KNB
//
//        val tableName = "actors"
//
//        DriverManager.getConnection(URL, props).use { connection ->
//            val df = DataFrame.readSqlTable(connection, tableName, 100).cast<ActorKDF>()
//            val result = df.filter { it[ActorKDF::id] in 11..19 }
//            result[0][1] shouldBe "Víctor"
//
//            val schema = DataFrame.getSchemaForSqlTable(connection, tableName)
//            schema.columns["id"]!!.type shouldBe typeOf<Int>()
//            schema.columns["first_name"]!!.type shouldBe typeOf<String?>()
//        }
//    }
//
//    @Test
//    fun `read table with schema name in table name`() {
//        val props = Properties()
//        props.setProperty("user", USER_NAME)
//        props.setProperty("password", PASSWORD)
//
//        // generate kdf schemas by database metadata (as interfaces or extensions)
//        // for gradle or as classes under the hood in KNB
//        val imdbTableName = "imdb.actors"
//
//        DriverManager.getConnection(URL2, props).use { connection ->
//            val df = DataFrame.readSqlTable(connection, imdbTableName, 100).cast<ActorKDF>()
//            val result = df.filter { it[ActorKDF::id] in 11..19 }
//            result[0][1] shouldBe "Víctor"
//
//            val schema = DataFrame.getSchemaForSqlTable(connection, imdbTableName)
//            schema.columns["id"]!!.type shouldBe typeOf<Int>()
//            schema.columns["first_name"]!!.type shouldBe typeOf<String?>()
//        }
//    }
//
//    @Test
//    fun `read sql query`() {
//        @Language("sql")
//        val sqlQuery =
//            """
//            select name, year, rank,
//            GROUP_CONCAT (genre) as "genres"
//            from movies join movies_directors on  movie_id = movies.id
//                 join directors on directors.id=director_id left join movies_genres on movies.id = movies_genres.movie_id
//            where directors.first_name = "Quentin" and directors.last_name = "Tarantino"
//            and movies.name is not null and movies.name is not null
//            group by name, year, rank
//            order by year
//            """.trimIndent()
//        val props = Properties()
//        props.setProperty("user", USER_NAME)
//        props.setProperty("password", PASSWORD)
//
//        // generate kdf schemas by database metadata (as interfaces or extensions)
//        // for gradle or as classes under the hood in KNB
//
//        DriverManager.getConnection(URL, props).use { connection ->
//            val df = DataFrame.readSqlQuery(connection, sqlQuery).cast<RankedMoviesWithGenres>()
//            val result =
//                df.filter { it[RankedMoviesWithGenres::year] != null && it[RankedMoviesWithGenres::year]!! > 2000 }
//            result[0][1] shouldBe 2003
//
//            val schema = DataFrame.getSchemaForSqlQuery(connection, sqlQuery)
//            schema.columns["name"]!!.type shouldBe typeOf<String?>()
//            schema.columns["year"]!!.type shouldBe typeOf<Int?>()
//        }
//    }
//}
