//package org.jetbrains.kotlinx.dataframe.io.h2
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
//import org.junit.Test
//import java.math.BigDecimal
//import java.sql.Connection
//import java.sql.DriverManager
//import java.sql.SQLException
//import java.util.Date
//import kotlin.reflect.typeOf
//
//// NOTE: the names of testing databases should be different to avoid collisions and should not contain the system names itself
//private const val URL = "jdbc:h2:mem:test2;DB_CLOSE_DELAY=-1;MODE=MySQL;DATABASE_TO_LOWER=TRUE"
//
//@DataSchema
//interface Table1MySql {
//    val id: Int
//    val bitCol: Boolean
//    val tinyintcol: Int
//    val smallintcol: Int
//    val mediumintcol: Int
//    val mediumintunsignedcol: Int
//    val integercol: Int
//    val intcol: Int
//    val integerunsignedcol: Long
//    val bigintcol: Long
//    val floatcol: Float
//    val doublecol: Double
//    val decimalcol: BigDecimal
//    val datecol: String
//    val datetimecol: String
//    val timestampcol: String
//    val timecol: String
//    val yearcol: String
//    val varcharcol: String
//    val charcol: String
//    val binarycol: ByteArray
//    val varbinarycol: ByteArray
//    val tinyblobcol: ByteArray
//    val blobcol: ByteArray
//    val mediumblobcol: ByteArray
//    val longblobcol: ByteArray
//    val textcol: String
//    val mediumtextcol: String
//    val longtextcol: String
//    val enumcol: String
//    val setcol: Char
//}
//
//@DataSchema
//interface Table2MySql {
//    val id: Int
//    val bitcol: Boolean?
//    val tinyintcol: Int?
//    val smallintcol: Int?
//    val mediumintcol: Int?
//    val mediumintUnsignedcol: Int?
//    val integercol: Int?
//    val intcol: Int?
//    val integerUnsignedcol: Long?
//    val bigintcol: Long?
//    val floatcol: Float?
//    val doublecol: Double?
//    val decimalcol: Double?
//    val datecol: String?
//    val datetimecol: String?
//    val timestampcol: String?
//    val timecol: String?
//    val yearcol: String?
//    val varcharcol: String?
//    val charcol: String?
//    val binarycol: ByteArray?
//    val varbinarycol: ByteArray?
//    val tinyblobcol: ByteArray?
//    val blobcol: ByteArray?
//    val mediumblobcol: ByteArray?
//    val longblobcol: ByteArray?
//    val textcol: String?
//    val mediumtextcol: String?
//    val longtextcol: String?
//    val enumcol: String?
//    val setcol: Char?
//    val jsoncol: String?
//}
//
//@DataSchema
//interface Table3MySql {
//    val id: Int
//    val enumcol: String
//}
//
//class MySqlH2Test {
//    companion object {
//        private lateinit var connection: Connection
//
//        @BeforeClass
//        @JvmStatic
//        fun setUpClass() {
//            connection = DriverManager.getConnection(URL)
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
//                data JSON
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
//                data JSON
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
//                    mediumblobCol, longblobCol, textCol, mediumtextCol, longtextCol, enumCol, data
//                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
//                """.trimIndent()
//
//            @Language("SQL")
//            val insertData2 =
//                """
//                INSERT INTO table2 (
//                    bitCol, tinyintCol, smallintCol, mediumintCol, mediumintUnsignedCol, integerCol, intCol,
//                    integerUnsignedCol, bigintCol, floatCol, doubleCol, decimalCol, dateCol, datetimeCol, timestampCol,
//                    timeCol, yearCol, varcharCol, charCol, binaryCol, varbinaryCol, tinyblobCol, blobCol,
//                    mediumblobCol, longblobCol, textCol, mediumtextCol, longtextCol, enumCol, data
//                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
//                    st.setDate(13, java.sql.Date(System.currentTimeMillis()))
//                    st.setTimestamp(14, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTimestamp(15, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTime(16, java.sql.Time(System.currentTimeMillis()))
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
//                    st.setString(30, "{\"key\": \"value\"}")
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
//                    st.setDate(13, java.sql.Date(System.currentTimeMillis()))
//                    st.setTimestamp(14, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTimestamp(15, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTime(16, java.sql.Time(System.currentTimeMillis()))
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
//                    st.setString(30, "{\"key\": \"value\"}")
//                    st.executeUpdate()
//                }
//            }
//        }
//
//        @AfterClass
//        @JvmStatic
//        fun tearDownClass() {
//            try {
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
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, "table1")
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["textcol"]!!.type shouldBe typeOf<String>()
//        schema.columns["datecol"]!!.type shouldBe typeOf<Date>()
//        schema.columns["datetimecol"]!!.type shouldBe typeOf<java.sql.Timestamp>()
//        schema.columns["timestampcol"]!!.type shouldBe typeOf<java.sql.Timestamp>()
//        schema.columns["timecol"]!!.type shouldBe typeOf<java.sql.Time>()
//        schema.columns["yearcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["varbinarycol"]!!.type shouldBe typeOf<ByteArray>()
//        schema.columns["binarycol"]!!.type shouldBe typeOf<ByteArray>()
//        schema.columns["longblobcol"]!!.type shouldBe typeOf<java.sql.Blob>()
//        schema.columns["tinyblobcol"]!!.type shouldBe typeOf<java.sql.Blob>()
//
//        val df2 = DataFrame.readSqlTable(connection, "table2").cast<Table2MySql>()
//        val result2 = df2.filter { it[Table2MySql::id] == 1 }
//        result2[0][26] shouldBe null
//
//        val schema2 = DataFrame.getSchemaForSqlTable(connection, "table2")
//        schema2.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema2.columns["textcol"]!!.type shouldBe typeOf<String?>()
//    }
//
//    @Test
//    fun `read from sql query`() {
//        @Language("SQL")
//        val sqlQuery =
//            """
//            SELECT
//               t1.id,
//               t1.enumCol
//            FROM table1 t1
//            JOIN table2 t2 ON t1.id = t2.id
//            """.trimIndent()
//
//        val df = DataFrame.readSqlQuery(connection, sqlQuery = sqlQuery).cast<Table3MySql>()
//        val result = df.filter { it[Table3MySql::id] == 1 }
//        result[0][1] shouldBe "Value1"
//
//        val schema = DataFrame.getSchemaForSqlQuery(connection, sqlQuery = sqlQuery)
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["enumcol"]!!.type shouldBe typeOf<Any>()
//    }
//
//    @Test
//    fun `read from all tables`() {
//        val dataframes = DataFrame.readAllSqlTables(connection).values.toList()
//
//        val table1Df = dataframes[0].cast<Table1MySql>()
//
//        table1Df.rowsCount() shouldBe 3
//        table1Df.filter { it[Table1MySql::integercol] > 100 }.rowsCount() shouldBe 2
//        table1Df[0][11] shouldBe 10.0
//        table1Df[0][26] shouldBe "textValue1"
//
//        val table2Df = dataframes[1].cast<Table2MySql>()
//
//        table2Df.rowsCount() shouldBe 3
//        table2Df.filter {
//            it[Table2MySql::integercol] != null && it[Table2MySql::integercol]!! > 400
//        }.rowsCount() shouldBe 1
//        table2Df[0][11] shouldBe 20.0
//        table2Df[0][26] shouldBe null
//    }
//
//    @Test
//    fun `reading numeric types`() {
//        val df1 = DataFrame.readSqlTable(connection, "table1").cast<Table1MySql>()
//
//        val result = df1.select("tinyintcol").add("tinyintcol2") { it[Table1MySql::tinyintcol] }
//
//        result[0][1] shouldBe 1.toByte()
//
//        val result1 = df1.select("smallintcol")
//            .add("smallintcol2") { it[Table1MySql::smallintcol] }
//
//        result1[0][1] shouldBe 10.toShort()
//
//        val result2 = df1.select("mediumintcol")
//            .add("mediumintcol2") { it[Table1MySql::mediumintcol] }
//
//        result2[0][1] shouldBe 100
//
//        val result3 = df1.select("mediumintunsignedcol")
//            .add("mediumintunsignedcol2") { it[Table1MySql::mediumintunsignedcol] }
//
//        result3[0][1] shouldBe 100
//
//        val result5 = df1.select("bigintcol")
//            .add("bigintcol2") { it[Table1MySql::bigintcol] }
//
//        result5[0][1] shouldBe 100
//
//        val result7 = df1.select("doublecol")
//            .add("doublecol2") { it[Table1MySql::doublecol] }
//
//        result7[0][1] shouldBe 10.0
//
//        val result8 = df1.select("decimalcol")
//            .add("decimalcol2") { it[Table1MySql::decimalcol] }
//
//        result8[0][1] shouldBe BigDecimal("10")
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, "table1")
//
//        schema.columns["tinyintcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["smallintcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["mediumintcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["mediumintunsignedcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["bigintcol"]!!.type shouldBe typeOf<Long>()
//        schema.columns["floatcol"]!!.type shouldBe typeOf<Double>()
//        schema.columns["doublecol"]!!.type shouldBe typeOf<Double>()
//        schema.columns["decimalcol"]!!.type shouldBe typeOf<BigDecimal>()
//    }
//
//    @Test
//    fun `infer nullability`() {
//        inferNullability(connection)
//    }
//}
