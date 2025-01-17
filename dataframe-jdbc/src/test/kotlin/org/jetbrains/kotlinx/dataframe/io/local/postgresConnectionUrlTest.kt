//package org.jetbrains.kotlinx.dataframe.io.local
//
//import io.kotest.assertions.throwables.shouldThrow
//import io.kotest.matchers.shouldBe
//import org.jetbrains.kotlinx.dataframe.DataFrame
//import org.jetbrains.kotlinx.dataframe.api.cast
//import org.jetbrains.kotlinx.dataframe.api.filter
//import org.jetbrains.kotlinx.dataframe.io.DbConnectionConfig
//import org.jetbrains.kotlinx.dataframe.io.readDataFrame
//import org.jetbrains.kotlinx.dataframe.io.readSqlTable
//import org.junit.Ignore
//import org.junit.Test
//import java.sql.DriverManager
//
//private const val URL_WITH_LOGIN_PASSWORD = "jdbc:postgresql://localhost:5432/test?" +
//    "user=postgres&password=pass&connectTimeout=10&tcpKeepAlive=true"
//
//private const val URL_NO_LOGIN_PASSWORD = "jdbc:postgresql://localhost:5432/test?connectTimeout=10&tcpKeepAlive=true"
//
//private const val URL_WITH_PASSWORD =
//    "jdbc:postgresql://localhost:5432/test?password=pass&connectTimeout=10&tcpKeepAlive=true"
//
//private const val URL_WITH_LOGIN =
//    "jdbc:postgresql://localhost:5432/test?user=postgres&connectTimeout=10&tcpKeepAlive=true"
//
//private const val TABLE_NAME = "table1"
//
//@Ignore
//class PostgresConnectionUrlTest {
//    @Test
//    fun `read from table with login and password in connection URL`() {
//        DriverManager.getConnection(URL_WITH_LOGIN_PASSWORD).use { connection ->
//            createTestData(connection)
//
//            val df1 = DataFrame.readSqlTable(connection, TABLE_NAME).cast<Table1>()
//            val result1 = df1.filter { it[Table1::id] == 1 }
//
//            result1[0][2] shouldBe 11
//
//            val df2 = connection.readDataFrame(TABLE_NAME).cast<Table1>()
//            val result2 = df2.filter { it[Table1::id] == 1 }
//
//            result2[0][2] shouldBe 11
//
//            clearTestData(connection)
//        }
//    }
//
//    @Test
//    fun `read from table with login and password in connection URL for DBConfig`() {
//        DriverManager.getConnection(URL_WITH_LOGIN_PASSWORD).use { connection ->
//            createTestData(connection)
//
//            val dbConfig = DbConnectionConfig(URL_WITH_LOGIN_PASSWORD)
//            val df1 = DataFrame.readSqlTable(dbConfig = dbConfig, TABLE_NAME).cast<Table1>()
//            val result1 = df1.filter { it[Table1::id] == 1 }
//
//            result1[0][2] shouldBe 11
//
//            val df2 = dbConfig.readDataFrame(TABLE_NAME).cast<Table1>()
//            val result2 = df2.filter { it[Table1::id] == 1 }
//
//            result2[0][2] shouldBe 11
//
//            clearTestData(connection)
//        }
//    }
//
//    @Test
//    fun `read from table without login and password`() {
//        val dbConfig = DbConnectionConfig(URL_NO_LOGIN_PASSWORD)
//
//        shouldThrow<org.postgresql.util.PSQLException> {
//            testReadFromTable(dbConfig)
//        }
//    }
//
//    @Test
//    fun `read from table with password only`() {
//        val dbConfig = DbConnectionConfig(URL_WITH_PASSWORD)
//
//        shouldThrow<org.postgresql.util.PSQLException> {
//            testReadFromTable(dbConfig)
//        }
//    }
//
//    @Test
//    fun `read from table with login only`() {
//        val dbConfig = DbConnectionConfig(URL_WITH_LOGIN)
//
//        shouldThrow<org.postgresql.util.PSQLException> {
//            testReadFromTable(dbConfig)
//        }
//    }
//
//    private fun testReadFromTable(dbConfig: DbConnectionConfig) {
//        DriverManager.getConnection(URL_WITH_LOGIN_PASSWORD).use { connection ->
//            createTestData(connection)
//
//            val df2 = dbConfig.readDataFrame(TABLE_NAME).cast<Table1>()
//            val result2 = df2.filter { it[Table1::id] == 1 }
//
//            result2[0][2] shouldBe 11
//
//            clearTestData(connection)
//        }
//    }
//}
