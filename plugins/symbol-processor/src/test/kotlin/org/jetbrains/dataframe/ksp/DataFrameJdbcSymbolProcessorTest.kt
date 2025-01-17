package org.jetbrains.dataframe.ksp

import com.tschuchort.compiletesting.SourceFile
import io.kotest.assertions.asClue
import io.kotest.inspectors.forAtLeastOne
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.shouldContain
import org.jetbrains.dataframe.ksp.runner.KotlinCompileTestingCompilationResult
import org.jetbrains.dataframe.ksp.runner.KspCompilationTestRunner
import org.jetbrains.dataframe.ksp.runner.TestCompilationParameters
import org.junit.AfterClass
import org.junit.Before
import org.junit.BeforeClass
import java.sql.Connection
import java.sql.DriverManager
import java.sql.SQLException
import kotlin.test.Test

const val CONNECTION_URL = "jdbc:h2:mem:test;DB_CLOSE_DELAY=-1;MODE=MySQL;DATABASE_TO_UPPER=false"

@Suppress("unused")
class DataFrameJdbcSymbolProcessorTest {
    companion object {
        private lateinit var connection: Connection

        val imports =
            """
            import org.jetbrains.kotlinx.dataframe.annotations.*
            import org.jetbrains.kotlinx.dataframe.columns.*
            import org.jetbrains.kotlinx.dataframe.* 
            """.trimIndent()

        const val GENERATED_FILE = "HelloJdbc${'$'}Extensions.kt"

        @JvmStatic
        @BeforeClass
        fun setupDB() {
            connection = DriverManager.getConnection(CONNECTION_URL)
            createTestDatabase(connection)
        }

        @JvmStatic
        @AfterClass
        fun close() {
            try {
                connection.close()
            } catch (e: SQLException) {
                e.printStackTrace()
            }
        }

        private fun createTestDatabase(connection: Connection) {
            // Crate table Customer
            connection.createStatement().execute(
                """
                CREATE TABLE Customer (
                    id INT PRIMARY KEY,
                    name VARCHAR(50),
                    age INT
                )
                """.trimIndent(),
            )

            // Create table Sale
            connection.createStatement().execute(
                """
                CREATE TABLE Sale (
                    id INT PRIMARY KEY,
                    customerId INT,
                    amount DECIMAL(10, 2)
                )
                """.trimIndent(),
            )

            // add data to the Customer table
            connection.createStatement().execute("INSERT INTO Customer (id, name, age) VALUES (1, 'John', 40)")
            connection.createStatement().execute("INSERT INTO Customer (id, name, age) VALUES (2, 'Alice', 25)")
            connection.createStatement().execute("INSERT INTO Customer (id, name, age) VALUES (3, 'Bob', 47)")

            // add data to the Sale table
            connection.createStatement().execute("INSERT INTO Sale (id, customerId, amount) VALUES (1, 1, 100.50)")
            connection.createStatement().execute("INSERT INTO Sale (id, customerId, amount) VALUES (2, 2, 50.00)")
            connection.createStatement().execute("INSERT INTO Sale (id, customerId, amount) VALUES (3, 1, 75.25)")
            connection.createStatement().execute("INSERT INTO Sale (id, customerId, amount) VALUES (4, 3, 35.15)")
        }
    }

    @Before
    fun setup() {
        KspCompilationTestRunner.compilationDir.deleteRecursively()
    }

    @Test
    fun `failed compilation on wrong `() {
        val result = KspCompilationTestRunner.compile(
            TestCompilationParameters(
                sources = listOf(
                    SourceFile.kotlin(
                        "MySources.kt",
                        """
                        @file:ImportDataSchema(name = "Customer", path = "123")
                        
                        package test
                        
                        import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
                        import org.jetbrains.kotlinx.dataframe.annotations.JdbcOptions
                        import org.jetbrains.kotlinx.dataframe.api.filter
                        import org.jetbrains.kotlinx.dataframe.DataFrame
                        import org.jetbrains.kotlinx.dataframe.api.cast
                        import java.sql.Connection
                        import java.sql.DriverManager
                        import java.sql.SQLException
                        import org.jetbrains.kotlinx.dataframe.io.readSqlTable
                        import org.jetbrains.kotlinx.dataframe.io.DbConnectionConfig
                        """.trimIndent(),
                    ),
                ),
            ),
        )
        result.successfulCompilation shouldBe false
    }

    @Test
    fun `schema is imported`() {
        val result = KspCompilationTestRunner.compile(
            TestCompilationParameters(
                sources = listOf(
                    SourceFile.kotlin(
                        "MySources.kt",
                        """
                        @file:ImportDataSchema(
                             "Customer",
                             "$CONNECTION_URL",
                             jdbcOptions = JdbcOptions("", "", tableName = "Customer")
                        )
                        
                        package test
                        
                        import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
                        import org.jetbrains.kotlinx.dataframe.annotations.JdbcOptions
                        import org.jetbrains.kotlinx.dataframe.api.filter
                        import org.jetbrains.kotlinx.dataframe.DataFrame
                        import org.jetbrains.kotlinx.dataframe.api.cast
                        import java.sql.Connection
                        import java.sql.DriverManager
                        import java.sql.SQLException
                        import org.jetbrains.kotlinx.dataframe.io.readSqlTable
                        import org.jetbrains.kotlinx.dataframe.io.DbConnectionConfig
                        """.trimIndent(),
                    ),
                ),
            ),
        )
        println(result.kspGeneratedFiles)
        result.inspectLines("Customer.Generated.kt") {
            it.forAtLeastOne { it shouldContain "val name: String" }
        }
    }

    /**
     * Test code is copied from h2Test `read from table` test.
     */
    fun `schema extracted via readFromDB method is resolved`() {
        val result = KspCompilationTestRunner.compile(
            TestCompilationParameters(
                sources = listOf(
                    SourceFile.kotlin(
                        "MySources.kt",
                        """
                        @file:ImportDataSchema(
                             "Customer",
                             "$CONNECTION_URL",
                             jdbcOptions = JdbcOptions("", "", tableName = "Customer")
                        )
                        
                        package test
                        
                        import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
                        import org.jetbrains.kotlinx.dataframe.annotations.JdbcOptions
                        import org.jetbrains.kotlinx.dataframe.api.filter
                        import org.jetbrains.kotlinx.dataframe.DataFrame
                        import org.jetbrains.kotlinx.dataframe.api.cast
                        import java.sql.Connection
                        import java.sql.DriverManager
                        import java.sql.SQLException
                        import org.jetbrains.kotlinx.dataframe.io.readSqlTable
                        import org.jetbrains.kotlinx.dataframe.io.DbConnectionConfig
                        
                        fun main() {    
                            val tableName = "Customer"
                            DriverManager.getConnection("$CONNECTION_URL").use { connection ->
                                val df = DataFrame.readSqlTable(connection, tableName).cast<Customer>()
                                df.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }

                                val df1 = DataFrame.readSqlTable(connection, tableName, 1).cast<Customer>()
                                df1.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                                
                                val dbConfig = DbConnectionConfig(url = "$CONNECTION_URL")
                                val df2 = DataFrame.readSqlTable(dbConfig, tableName).cast<Customer>()
                                df2.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                                
                                val df3 = DataFrame.readSqlTable(dbConfig, tableName, 1).cast<Customer>()
                                df3.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                        
                            }
                        }
                        """.trimIndent(),
                    ),
                ),
            ),
        )
        result.successfulCompilation shouldBe true
    }

    /**
     * Test code is copied from test above.
     */
    fun `schema extracted via readFromDB method is resolved with db credentials from env variables`() {
        val result = KspCompilationTestRunner.compile(
            TestCompilationParameters(
                sources = listOf(
                    SourceFile.kotlin(
                        "MySources.kt",
                        """
                        @file:ImportDataSchema(
                             "Customer",
                             "$CONNECTION_URL",
                             jdbcOptions = JdbcOptions("", "", extractCredFromEnv = true, tableName = "Customer")
                        )
                        
                        package test
                        
                        import org.jetbrains.kotlinx.dataframe.annotations.ImportDataSchema
                        import org.jetbrains.kotlinx.dataframe.annotations.JdbcOptions
                        import org.jetbrains.kotlinx.dataframe.api.filter
                        import org.jetbrains.kotlinx.dataframe.DataFrame
                        import org.jetbrains.kotlinx.dataframe.api.cast
                        import java.sql.Connection
                        import java.sql.DriverManager
                        import java.sql.SQLException
                        import org.jetbrains.kotlinx.dataframe.io.readSqlTable
                        import org.jetbrains.kotlinx.dataframe.io.DbConnectionConfig
                        
                        fun main() {    
                            val tableName = "Customer"
                            DriverManager.getConnection("$CONNECTION_URL").use { connection ->
                                val df = DataFrame.readSqlTable(connection, tableName).cast<Customer>()
                                df.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }

                                val df1 = DataFrame.readSqlTable(connection, tableName, 1).cast<Customer>()
                                df1.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                                
                                val dbConfig = DbConnectionConfig(url = "$CONNECTION_URL")
                                val df2 = DataFrame.readSqlTable(dbConfig, tableName).cast<Customer>()
                                df2.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                                
                                val df3 = DataFrame.readSqlTable(dbConfig, tableName, 1).cast<Customer>()
                                df3.filter { it[Customer::age] != null && it[Customer::age]!! > 30 }
                        
                            }
                        }
                        """.trimIndent(),
                    ),
                ),
            ),
        )
        result.successfulCompilation shouldBe true
    }

    private fun KotlinCompileTestingCompilationResult.inspectLines(f: (List<String>) -> Unit) {
        inspectLines(GENERATED_FILE, f)
    }

    private fun KotlinCompileTestingCompilationResult.inspectLines(filename: String, f: (List<String>) -> Unit) {
        kspGeneratedFiles.single { it.name == filename }.readLines().asClue(f)
    }
}
