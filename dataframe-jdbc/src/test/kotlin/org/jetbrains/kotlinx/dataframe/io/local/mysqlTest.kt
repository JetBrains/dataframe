//package org.jetbrains.kotlinx.dataframe.io.local
//
//import io.kotest.matchers.shouldBe
//import org.intellij.lang.annotations.Language
//import org.jetbrains.kotlinx.dataframe.DataFrame
//import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
//import org.jetbrains.kotlinx.dataframe.api.add
//import org.jetbrains.kotlinx.dataframe.api.cast
//import org.jetbrains.kotlinx.dataframe.api.filter
//import org.jetbrains.kotlinx.dataframe.api.select
//import org.jetbrains.kotlinx.dataframe.io.getSchemaForSqlQuery
//import org.jetbrains.kotlinx.dataframe.io.getSchemaForSqlTable
//import org.jetbrains.kotlinx.dataframe.io.inferNullability
//import org.jetbrains.kotlinx.dataframe.io.readAllSqlTables
//import org.jetbrains.kotlinx.dataframe.io.readSqlQuery
//import org.jetbrains.kotlinx.dataframe.io.readSqlTable
//import org.junit.AfterClass
//import org.junit.BeforeClass
//import org.junit.Ignore
//import org.junit.Test
//import java.math.BigDecimal
//import java.sql.Connection
//import java.sql.DriverManager
//import java.sql.SQLException
//import java.util.Date
//import kotlin.reflect.typeOf
//
//private const val URL = "jdbc:mysql://localhost:3306"
//private const val USER_NAME = "root"
//private const val PASSWORD = "pass"
//private const val TEST_DATABASE_NAME = "testKDFdatabase"
//private const val TIMESTAMP = 1726246245460
//
//@DataSchema
//interface Table1MySql {
//    val id: Int
//    val bitCol: Boolean
//    val tinyintCol: Int
//    val smallintCol: Int
//    val mediumintCol: Int
//    val mediumintUnsignedCol: Int
//    val integerCol: Int
//    val intCol: Int
//    val integerUnsignedCol: Long
//    val bigintCol: Long
//    val floatCol: Float
//    val doubleCol: Double
//    val decimalCol: BigDecimal
//    val dateCol: String
//    val datetimeCol: String
//    val timestampCol: String
//    val timeCol: String
//    val yearCol: String
//    val varcharCol: String
//    val charCol: String
//    val binaryCol: ByteArray
//    val varbinaryCol: ByteArray
//    val tinyblobCol: ByteArray
//    val blobCol: ByteArray
//    val mediumblobCol: ByteArray
//    val longblobCol: ByteArray
//    val textCol: String
//    val mediumtextCol: String
//    val longtextCol: String
//    val enumCol: String
//    val setCol: Char
//}
//
//@DataSchema
//interface Table2MySql {
//    val id: Int
//    val bitCol: Boolean?
//    val tinyintCol: Int?
//    val smallintCol: Int?
//    val mediumintCol: Int?
//    val mediumintUnsignedCol: Int?
//    val integerCol: Int?
//    val intCol: Int?
//    val integerUnsignedCol: Long?
//    val bigintCol: Long?
//    val floatCol: Float?
//    val doubleCol: Double?
//    val decimalCol: Double?
//    val dateCol: String?
//    val datetimeCol: String?
//    val timestampCol: String?
//    val timeCol: String?
//    val yearCol: String?
//    val varcharCol: String?
//    val charCol: String?
//    val binaryCol: ByteArray?
//    val varbinaryCol: ByteArray?
//    val tinyblobCol: ByteArray?
//    val blobCol: ByteArray?
//    val mediumblobCol: ByteArray?
//    val longblobCol: ByteArray?
//    val textCol: String?
//    val mediumtextCol: String?
//    val longtextCol: String?
//    val enumCol: String?
//    val setCol: Char?
//    val jsonCol: String?
//}
//
//@DataSchema
//interface Table3MySql {
//    val id: Int
//    val enumCol: String
//    val setCol: Char?
//}
//
//@Ignore
//class MySqlTest {
//    companion object {
//        private lateinit var connection: Connection
//
//        @BeforeClass
//        @JvmStatic
//        fun setUpClass() {
//            connection = DriverManager.getConnection(URL, USER_NAME, PASSWORD)
//
//            connection.createStatement().use { st ->
//                // Drop the test database if it exists
//                val dropDatabaseQuery = "DROP DATABASE IF EXISTS $TEST_DATABASE_NAME"
//                st.executeUpdate(dropDatabaseQuery)
//
//                // Create the test database
//                val createDatabaseQuery = "CREATE DATABASE $TEST_DATABASE_NAME"
//                st.executeUpdate(createDatabaseQuery)
//
//                // Use the newly created database
//                val useDatabaseQuery = "USE $TEST_DATABASE_NAME"
//                st.executeUpdate(useDatabaseQuery)
//            }
//
//            connection.createStatement().use { st -> st.execute("DROP TABLE IF EXISTS table1") }
//            connection.createStatement().use { st -> st.execute("DROP TABLE IF EXISTS table2") }
//
//            @Language("SQL")
//            val createTableQuery = """
//            CREATE TABLE IF NOT EXISTS table1 (
//                id INT AUTO_INCREMENT PRIMARY KEY,
//                bitCol BIT NOT NULL,
//                tinyintCol TINYINT NOT NULL,
//                smallintCol SMALLINT NOT NULL,
//                mediumintCol MEDIUMINT NOT NULL,
//                mediumintUnsignedCol MEDIUMINT UNSIGNED NOT NULL,
//                integerCol INTEGER NOT NULL,
//                intCol INT NOT NULL,
//                integerUnsignedCol INTEGER UNSIGNED NOT NULL,
//                bigintCol BIGINT NOT NULL,
//                floatCol FLOAT NOT NULL,
//                doubleCol DOUBLE NOT NULL,
//                decimalCol DECIMAL NOT NULL,
//                dateCol DATE NOT NULL,
//                datetimeCol DATETIME NOT NULL,
//                timestampCol TIMESTAMP NOT NULL,
//                timeCol TIME NOT NULL,
//                yearCol YEAR NOT NULL,
//                varcharCol VARCHAR(255) NOT NULL,
//                charCol CHAR(10) NOT NULL,
//                binaryCol BINARY(64) NOT NULL,
//                varbinaryCol VARBINARY(128) NOT NULL,
//                tinyblobCol TINYBLOB NOT NULL,
//                blobCol BLOB NOT NULL,
//                mediumblobCol MEDIUMBLOB NOT NULL ,
//                longblobCol LONGBLOB NOT NULL,
//                textCol TEXT NOT NULL,
//                mediumtextCol MEDIUMTEXT NOT NULL,
//                longtextCol LONGTEXT NOT NULL,
//                enumCol ENUM('Value1', 'Value2', 'Value3') NOT NULL,
//                setCol SET('Option1', 'Option2', 'Option3') NOT NULL,
//                location GEOMETRY,
//                data JSON
//                CHECK (JSON_VALID(data))
//            )
//            """
//
//            connection.createStatement().execute(createTableQuery.trimIndent())
//
//            @Language("SQL")
//            val createTableQuery2 = """
//                CREATE TABLE IF NOT EXISTS table2 (
//                id INT AUTO_INCREMENT PRIMARY KEY,
//                bitCol BIT,
//                tinyintCol TINYINT,
//                smallintCol SMALLINT,
//                mediumintCol MEDIUMINT,
//                mediumintUnsignedCol MEDIUMINT UNSIGNED,
//                integerCol INTEGER,
//                intCol INT,
//                integerUnsignedCol INTEGER UNSIGNED,
//                bigintCol BIGINT,
//                floatCol FLOAT,
//                doubleCol DOUBLE,
//                decimalCol DECIMAL,
//                dateCol DATE,
//                datetimeCol DATETIME,
//                timestampCol TIMESTAMP,
//                timeCol TIME,
//                yearCol YEAR,
//                varcharCol VARCHAR(255),
//                charCol CHAR(10),
//                binaryCol BINARY(64),
//                varbinaryCol VARBINARY(128),
//                tinyblobCol TINYBLOB,
//                blobCol BLOB,
//                mediumblobCol MEDIUMBLOB,
//                longblobCol LONGBLOB,
//                textCol TEXT,
//                mediumtextCol MEDIUMTEXT,
//                longtextCol LONGTEXT,
//                enumCol ENUM('Value1', 'Value2', 'Value3'),
//                setCol SET('Option1', 'Option2', 'Option3'),
//                location GEOMETRY,
//                data JSON
//                CHECK (JSON_VALID(data))
//            )
//            """
//
//            connection.createStatement().execute(createTableQuery2.trimIndent())
//
//            @Language("SQL")
//            val insertData1 =
//                """
//                INSERT INTO table1 (
//                    bitCol, tinyintCol, smallintCol, mediumintCol, mediumintUnsignedCol, integerCol, intCol,
//                    integerUnsignedCol, bigintCol, floatCol, doubleCol, decimalCol, dateCol, datetimeCol, timestampCol,
//                    timeCol, yearCol, varcharCol, charCol, binaryCol, varbinaryCol, tinyblobCol, blobCol,
//                    mediumblobCol, longblobCol, textCol, mediumtextCol, longtextCol, enumCol, setCol, location, data
//                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText('POINT(1 1)'), ?)
//                """.trimIndent()
//
//            @Language("SQL")
//            val insertData2 =
//                """
//                INSERT INTO table2 (
//                    bitCol, tinyintCol, smallintCol, mediumintCol, mediumintUnsignedCol, integerCol, intCol,
//                    integerUnsignedCol, bigintCol, floatCol, doubleCol, decimalCol, dateCol, datetimeCol, timestampCol,
//                    timeCol, yearCol, varcharCol, charCol, binaryCol, varbinaryCol, tinyblobCol, blobCol,
//                    mediumblobCol, longblobCol, textCol, mediumtextCol, longtextCol, enumCol, setCol, location, data
//                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ST_GeomFromText('POINT(1 1)'), ?)
//                """.trimIndent()
//
//            connection.prepareStatement(insertData1).use { st ->
//                // Insert data into table1
//                for (i in 1..3) {
//                    st.setBoolean(1, true)
//                    st.setByte(2, i.toByte())
//                    st.setShort(3, (i * 10).toShort())
//                    st.setInt(4, i * 100)
//                    st.setInt(5, i * 100)
//                    st.setInt(6, i * 100)
//                    st.setInt(7, i * 100)
//                    st.setInt(8, i * 100)
//                    st.setInt(9, i * 100)
//                    st.setFloat(10, i * 10.0f)
//                    st.setDouble(11, i * 10.0)
//                    st.setBigDecimal(12, BigDecimal(i * 10))
//                    st.setDate(13, java.sql.Date(TIMESTAMP))
//                    st.setTimestamp(14, java.sql.Timestamp(TIMESTAMP))
//                    st.setTimestamp(15, java.sql.Timestamp(TIMESTAMP))
//                    st.setTime(16, java.sql.Time(TIMESTAMP))
//                    st.setInt(17, 2023)
//                    st.setString(18, "varcharValue$i")
//                    st.setString(19, "charValue$i")
//                    st.setBytes(20, "binaryValue".toByteArray())
//                    st.setBytes(21, "varbinaryValue".toByteArray())
//                    st.setBytes(22, "tinyblobValue".toByteArray())
//                    st.setBytes(23, "blobValue".toByteArray())
//                    st.setBytes(24, "mediumblobValue".toByteArray())
//                    st.setBytes(25, "longblobValue".toByteArray())
//                    st.setString(26, "textValue$i")
//                    st.setString(27, "mediumtextValue$i")
//                    st.setString(28, "longtextValue$i")
//                    st.setString(29, "Value$i")
//                    st.setString(30, "Option$i")
//                    st.setString(31, "{\"key\": \"value\"}")
//                    st.executeUpdate()
//                }
//            }
//
//            connection.prepareStatement(insertData2).use { st ->
//                // Insert data into table2
//                for (i in 1..3) {
//                    st.setBoolean(1, false)
//                    st.setByte(2, (i * 2).toByte())
//                    st.setShort(3, (i * 20).toShort())
//                    st.setInt(4, i * 200)
//                    st.setInt(5, i * 200)
//                    st.setInt(6, i * 200)
//                    st.setInt(7, i * 200)
//                    st.setInt(8, i * 200)
//                    st.setInt(9, i * 200)
//                    st.setFloat(10, i * 20.0f)
//                    st.setDouble(11, i * 20.0)
//                    st.setBigDecimal(12, BigDecimal(i * 20))
//                    st.setDate(13, java.sql.Date(TIMESTAMP))
//                    st.setTimestamp(14, java.sql.Timestamp(TIMESTAMP))
//                    st.setTimestamp(15, java.sql.Timestamp(TIMESTAMP))
//                    st.setTime(16, java.sql.Time(TIMESTAMP))
//                    st.setInt(17, 2023)
//                    st.setString(18, "varcharValue$i")
//                    st.setString(19, "charValue$i")
//                    st.setBytes(20, "binaryValue".toByteArray())
//                    st.setBytes(21, "varbinaryValue".toByteArray())
//                    st.setBytes(22, "tinyblobValue".toByteArray())
//                    st.setBytes(23, "blobValue".toByteArray())
//                    st.setBytes(24, "mediumblobValue".toByteArray())
//                    st.setBytes(25, "longblobValue".toByteArray())
//                    st.setString(26, null)
//                    st.setString(27, null)
//                    st.setString(28, "longtextValue$i")
//                    st.setString(29, "Value$i")
//                    st.setString(30, "Option$i")
//                    st.setString(31, "{\"key\": \"value\"}")
//                    st.executeUpdate()
//                }
//            }
//        }
//
//        @AfterClass
//        @JvmStatic
//        fun tearDownClass() {
//            try {
//                connection.createStatement().use { st -> st.execute("DROP TABLE IF EXISTS table1") }
//                connection.createStatement().use { st -> st.execute("DROP TABLE IF EXISTS table2") }
//                connection.createStatement().use { st -> st.execute("DROP DATABASE IF EXISTS $TEST_DATABASE_NAME") }
//                connection.close()
//            } catch (e: SQLException) {
//                e.printStackTrace()
//            }
//        }
//    }
//
//    @Test
//    fun `basic test for reading sql tables`() {
//        val df1 = DataFrame.readSqlTable(connection, "table1").cast<Table1MySql>()
//        val result = df1.filter { it[Table1MySql::id] == 1 }
//        result[0][26] shouldBe "textValue1"
//        result[0][22] shouldBe "tinyblobValue".toByteArray()
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, "table1")
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["textCol"]!!.type shouldBe typeOf<String>()
//        schema.columns["dateCol"]!!.type shouldBe typeOf<Date>()
//        schema.columns["datetimeCol"]!!.type shouldBe typeOf<java.time.LocalDateTime>()
//        schema.columns["timestampCol"]!!.type shouldBe typeOf<java.sql.Timestamp>()
//        schema.columns["timeCol"]!!.type shouldBe typeOf<java.sql.Time>()
//        schema.columns["yearCol"]!!.type shouldBe typeOf<Date>()
//        schema.columns["textCol"]!!.type shouldBe typeOf<String>()
//        schema.columns["varbinaryCol"]!!.type shouldBe typeOf<ByteArray>()
//        schema.columns["binaryCol"]!!.type shouldBe typeOf<ByteArray>()
//        schema.columns["longblobCol"]!!.type shouldBe typeOf<ByteArray>()
//        schema.columns["tinyblobCol"]!!.type shouldBe typeOf<ByteArray>()
//
//        val df2 = DataFrame.readSqlTable(connection, "table2").cast<Table2MySql>()
//        val result2 = df2.filter { it[Table2MySql::id] == 1 }
//        result2[0][26] shouldBe null
//
//        val schema2 = DataFrame.getSchemaForSqlTable(connection, "table2")
//        schema2.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema2.columns["textCol"]!!.type shouldBe typeOf<String?>()
//    }
//
//    @Test
//    fun `read from sql query`() {
//        @Language("SQL")
//        val sqlQuery =
//            """
//            SELECT
//               t1.id,
//               t1.enumCol,
//               t2.setCol
//            FROM table1 t1
//            JOIN table2 t2 ON t1.id = t2.id
//            """.trimIndent()
//
//        val df = DataFrame.readSqlQuery(connection, sqlQuery = sqlQuery).cast<Table3MySql>()
//        val result = df.filter { it[Table3MySql::id] == 1 }
//        result[0][2] shouldBe "Option1"
//
//        val schema = DataFrame.getSchemaForSqlQuery(connection, sqlQuery = sqlQuery)
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["enumCol"]!!.type shouldBe typeOf<String>()
//        schema.columns["setCol"]!!.type shouldBe typeOf<String?>()
//    }
//
//    @Test
//    fun `read from all tables`() {
//        val dataframes = DataFrame.readAllSqlTables(connection).values.toList()
//
//        val table1Df = dataframes[0].cast<Table1MySql>()
//
//        table1Df.rowsCount() shouldBe 3
//        table1Df.filter { it[Table1MySql::integerCol] > 100 }.rowsCount() shouldBe 2
//        table1Df[0][11] shouldBe 10.0
//        table1Df[0][26] shouldBe "textValue1"
//
//        val table2Df = dataframes[1].cast<Table2MySql>()
//
//        table2Df.rowsCount() shouldBe 3
//        table2Df.filter {
//            it[Table2MySql::integerCol] != null && it[Table2MySql::integerCol]!! > 400
//        }.rowsCount() shouldBe 1
//        table2Df[0][11] shouldBe 20.0
//        table2Df[0][26] shouldBe null
//    }
//
//    @Test
//    fun `reading numeric types`() {
//        val df1 = DataFrame.readSqlTable(connection, "table1").cast<Table1MySql>()
//
//        val result = df1.select("tinyintCol").add("tinyintCol2") { it[Table1MySql::tinyintCol] }
//
//        result[0][1] shouldBe 1.toByte()
//
//        val result1 = df1.select("smallintCol")
//            .add("smallintCol2") { it[Table1MySql::smallintCol] }
//
//        result1[0][1] shouldBe 10.toShort()
//
//        val result2 = df1.select("mediumintCol")
//            .add("mediumintCol2") { it[Table1MySql::mediumintCol] }
//
//        result2[0][1] shouldBe 100
//
//        val result3 = df1.select("mediumintUnsignedCol")
//            .add("mediumintUnsignedCol2") { it[Table1MySql::mediumintUnsignedCol] }
//
//        result3[0][1] shouldBe 100
//
//        val result4 = df1.select("integerUnsignedCol")
//            .add("integerUnsignedCol2") { it[Table1MySql::integerUnsignedCol] }
//
//        result4[0][1] shouldBe 100L
//
//        val result5 = df1.select("bigintCol")
//            .add("bigintCol2") { it[Table1MySql::bigintCol] }
//
//        result5[0][1] shouldBe 100
//
//        val result6 = df1.select("floatCol")
//            .add("floatCol2") { it[Table1MySql::floatCol] }
//
//        result6[0][1] shouldBe 10.0f
//
//        val result7 = df1.select("doubleCol")
//            .add("doubleCol2") { it[Table1MySql::doubleCol] }
//
//        result7[0][1] shouldBe 10.0
//
//        val result8 = df1.select("decimalCol")
//            .add("decimalCol2") { it[Table1MySql::decimalCol] }
//
//        result8[0][1] shouldBe BigDecimal("10")
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, "table1")
//
//        schema.columns["tinyintCol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["smallintCol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["mediumintCol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["mediumintUnsignedCol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["integerUnsignedCol"]!!.type shouldBe typeOf<Long>()
//        schema.columns["bigintCol"]!!.type shouldBe typeOf<Long>()
//        schema.columns["floatCol"]!!.type shouldBe typeOf<Float>()
//        schema.columns["doubleCol"]!!.type shouldBe typeOf<Double>()
//        schema.columns["decimalCol"]!!.type shouldBe typeOf<BigDecimal>()
//        // TODO: all unsigned types
//        // TODO: new mapping system based on class names
//        // validation after mapping in getObject
//        // getObject(i+1, type) catch getObject catch getString
//        // add direct mapping to getString and other methods
//    }
//
//    @Test
//    fun `infer nullability`() {
//        inferNullability(connection)
//    }
//}
