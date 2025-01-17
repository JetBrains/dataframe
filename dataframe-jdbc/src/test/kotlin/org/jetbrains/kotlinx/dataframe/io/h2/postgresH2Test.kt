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
//import java.util.UUID
//import kotlin.reflect.typeOf
//
//private const val URL =
//    "jdbc:h2:mem:test3;DB_CLOSE_DELAY=-1;MODE=PostgreSQL;DATABASE_TO_LOWER=TRUE;DEFAULT_NULL_ORDERING=HIGH"
//
//@DataSchema
//interface Table1 {
//    val id: Int
//    val bigintcol: Long
//    val smallintcol: Int
//    val bigserialcol: Long
//    val booleancol: Boolean
//    val byteacol: ByteArray
//    val charactercol: String
//    val characterncol: String
//    val charcol: String
//    val datecol: java.sql.Date
//    val doublecol: Double
//    val integercol: Int?
//    val jsoncol: String
//    val jsonbcol: String
//}
//
//@DataSchema
//interface Table2 {
//    val id: Int
//    val moneycol: String
//    val numericcol: BigDecimal
//    val realcol: Float
//    val smallintcol: Int
//    val serialcol: Int
//    val textcol: String?
//    val timecol: String
//    val timewithzonecol: String
//    val timestampcol: String
//    val timestampwithzonecol: String
//    val uuidcol: String
//}
//
//@DataSchema
//interface ViewTable {
//    val id: Int
//    val bigintcol: Long
//    val textCol: String?
//}
//
//class PostgresH2Test {
//    companion object {
//        private lateinit var connection: Connection
//
//        @BeforeClass
//        @JvmStatic
//        fun setUpClass() {
//            connection = DriverManager.getConnection(URL)
//
//            @Language("SQL")
//            val createTableStatement = """
//                CREATE TABLE IF NOT EXISTS table1 (
//                id serial PRIMARY KEY,
//                bigintCol bigint not null,
//                smallintCol smallint not null,
//                bigserialCol bigserial not null,
//                booleanCol boolean not null,
//                byteaCol bytea not null,
//                characterCol character not null,
//                characterNCol character(10) not null,
//                charCol char not null,
//                dateCol date not null,
//                doubleCol double precision not null,
//                integerCol integer,
//                intArrayCol integer array,
//                doubleArrayCol double precision array,
//                dateArrayCol date array,
//                textArrayCol text array,
//                booleanArrayCol boolean array
//            )
//            """
//            connection.createStatement().execute(createTableStatement.trimIndent())
//
//            @Language("SQL")
//            val createTableQuery = """
//                CREATE TABLE IF NOT EXISTS table2 (
//                id serial PRIMARY KEY,
//                moneyCol money not null,
//                numericCol numeric not null,
//                realCol real not null,
//                smallintCol smallint not null,
//                serialCol serial not null,
//                textCol text,
//                timeCol time not null,
//                timeWithZoneCol time with time zone not null,
//                timestampCol timestamp not null,
//                timestampWithZoneCol timestamp with time zone not null,
//                uuidCol uuid not null
//            )
//            """
//            connection.createStatement().execute(createTableQuery.trimIndent())
//
//            @Language("SQL")
//            val insertData1 = """
//            INSERT INTO table1 (
//                bigintCol, smallintCol, bigserialCol,  booleanCol,
//                byteaCol, characterCol, characterNCol, charCol,
//                dateCol, doubleCol,
//                integerCol, intArrayCol,
//                doubleArrayCol, dateArrayCol, textArrayCol, booleanArrayCol
//            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
//            """
//
//            @Language("SQL")
//            val insertData2 = """
//            INSERT INTO table2 (
//                moneyCol, numericCol,
//                realCol, smallintCol,
//                serialCol, textCol, timeCol,
//                timeWithZoneCol, timestampCol, timestampWithZoneCol,
//                uuidCol
//            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
//            """
//
//            val intArray = connection.createArrayOf("INTEGER", arrayOf(1, 2, 3))
//            val doubleArray = connection.createArrayOf("DOUBLE", arrayOf(1.1, 2.2, 3.3))
//            val dateArray = connection.createArrayOf(
//                "DATE",
//                arrayOf(java.sql.Date.valueOf("2023-08-01"), java.sql.Date.valueOf("2023-08-02")),
//            )
//            val textArray = connection.createArrayOf("TEXT", arrayOf("Hello", "World"))
//            val booleanArray = connection.createArrayOf("BOOLEAN", arrayOf(true, false, true))
//
//            connection.prepareStatement(insertData1).use { st ->
//                // Insert data into table1
//                for (i in 1..3) {
//                    st.setLong(1, i * 1000L)
//                    st.setShort(2, 11.toShort())
//                    st.setLong(3, 1000000000L + i)
//                    st.setBoolean(4, i % 2 == 1)
//                    st.setBytes(5, byteArrayOf(1, 2, 3))
//                    st.setString(6, "A")
//                    st.setString(7, "Hello")
//                    st.setString(8, "A")
//                    st.setDate(9, java.sql.Date.valueOf("2023-08-01"))
//                    st.setDouble(10, 12.34)
//                    st.setInt(11, 12345 * i)
//                    st.setArray(12, intArray)
//                    st.setArray(13, doubleArray)
//                    st.setArray(14, dateArray)
//                    st.setArray(15, textArray)
//                    st.setArray(16, booleanArray)
//                    st.executeUpdate()
//                }
//            }
//
//            connection.prepareStatement(insertData2).use { st ->
//                // Insert data into table2
//                for (i in 1..3) {
//                    st.setBigDecimal(1, BigDecimal("123.45"))
//                    st.setBigDecimal(2, BigDecimal("12.34"))
//                    st.setFloat(3, 12.34f)
//                    st.setInt(4, 1000 + i)
//                    st.setInt(5, 1000000 + i)
//                    st.setString(6, null)
//                    st.setTime(7, java.sql.Time.valueOf("12:34:56"))
//                    st.setTimestamp(8, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTimestamp(9, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setTimestamp(10, java.sql.Timestamp(System.currentTimeMillis()))
//                    st.setObject(11, UUID.randomUUID(), java.sql.Types.OTHER)
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
//    fun `read from tables`() {
//        val tableName1 = "table1"
//        val df1 = DataFrame.readSqlTable(connection, tableName1).cast<Table1>()
//        val result = df1.filter { it[Table1::id] == 1 }
//
//        result[0][0] shouldBe 1
//        result[0][8] shouldBe "A"
//        result[0][12] shouldBe arrayOf(1, 2, 3)
//        result[0][13] shouldBe arrayOf(1.1, 2.2, 3.3)
//        result[0][14] shouldBe arrayOf(java.sql.Date.valueOf("2023-08-01"), java.sql.Date.valueOf("2023-08-02"))
//        result[0][15] shouldBe arrayOf("Hello", "World")
//        result[0][16] shouldBe arrayOf(true, false, true)
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, tableName1)
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["integercol"]!!.type shouldBe typeOf<Int?>()
//        schema.columns["smallintcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["intarraycol"]!!.type.classifier shouldBe kotlin.Array::class
//        schema.columns["doublearraycol"]!!.type.classifier shouldBe kotlin.Array::class
//        schema.columns["datearraycol"]!!.type.classifier shouldBe kotlin.Array::class
//        schema.columns["textarraycol"]!!.type.classifier shouldBe kotlin.Array::class
//        schema.columns["booleanarraycol"]!!.type.classifier shouldBe kotlin.Array::class
//
//        val tableName2 = "table2"
//        val df2 = DataFrame.readSqlTable(connection, tableName2).cast<Table2>()
//        val result2 = df2.filter { it[Table2::id] == 1 }
//        result2[0][4] shouldBe 1001
//
//        val schema2 = DataFrame.getSchemaForSqlTable(connection, tableName2)
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
//                t1.id,
//                t1.bigintCol,
//                t2.textCol
//            FROM table1 t1
//            JOIN table2 t2 ON t1.id = t2.id
//            """.trimIndent()
//
//        val df = DataFrame.readSqlQuery(connection, sqlQuery = sqlQuery).cast<ViewTable>()
//        val result = df.filter { it[ViewTable::id] == 1 }
//        result[0][2] shouldBe null
//
//        val schema = DataFrame.getSchemaForSqlQuery(connection, sqlQuery = sqlQuery)
//        schema.columns["id"]!!.type shouldBe typeOf<Int>()
//        schema.columns["bigintcol"]!!.type shouldBe typeOf<Long>()
//        schema.columns["textcol"]!!.type shouldBe typeOf<String?>()
//    }
//
//    @Test
//    fun `read from all tables`() {
//        val dataframes = DataFrame.readAllSqlTables(connection).values.toList()
//
//        val table1Df = dataframes[0].cast<Table1>()
//
//        table1Df.rowsCount() shouldBe 3
//        table1Df.filter { it[Table1::integercol] != null && it[Table1::integercol]!! > 12345 }.rowsCount() shouldBe 2
//        table1Df[0][1] shouldBe 1000L
//        table1Df[0][2] shouldBe 11
//
//        val table2Df = dataframes[1].cast<Table2>()
//
//        table2Df.rowsCount() shouldBe 3
//        table2Df.filter {
//            it[Table2::realcol] == 12.34f
//        }.rowsCount() shouldBe 3
//        table2Df[0][4] shouldBe 1001
//    }
//
//    @Test
//    fun `read columns of different types to check type mapping`() {
//        val tableName1 = "table1"
//        val df1 = DataFrame.readSqlTable(connection, tableName1).cast<Table1>()
//        val result = df1.select("smallintcol")
//            .add("smallintcol2") { it[Table1::smallintcol] }
//        result[0][1] shouldBe 11
//
//        val result1 = df1.select("bigserialcol")
//            .add("bigserialcol2") { it[Table1::bigserialcol] }
//        result1[0][1] shouldBe 1000000001L
//
//        val result2 = df1.select("doublecol")
//            .add("doublecol2") { it[Table1::doublecol] }
//        result2[0][1] shouldBe 12.34
//
//        val tableName2 = "table2"
//        val df2 = DataFrame.readSqlTable(connection, tableName2).cast<Table2>()
//
//        val result4 = df2.select("numericcol")
//            .add("numericcol2") { it[Table2::numericcol] }
//        result4[0][1] shouldBe BigDecimal("12.34")
//
//        val result5 = df2.select("realcol")
//            .add("realcol2") { it[Table2::realcol] }
//        result5[0][1] shouldBe 12.34f
//
//        val result8 = df2.select("serialcol")
//            .add("serialcol2") { it[Table2::serialcol] }
//        result8[0][1] shouldBe 1000001
//
//        val schema = DataFrame.getSchemaForSqlTable(connection, tableName1)
//        schema.columns["smallintcol"]!!.type shouldBe typeOf<Int>()
//        schema.columns["bigserialcol"]!!.type shouldBe typeOf<Long>()
//        schema.columns["doublecol"]!!.type shouldBe typeOf<Double>()
//
//        val schema1 = DataFrame.getSchemaForSqlTable(connection, tableName2)
//        schema1.columns["numericcol"]!!.type shouldBe typeOf<BigDecimal>()
//        schema1.columns["realcol"]!!.type shouldBe typeOf<Float>()
//        schema1.columns["serialcol"]!!.type shouldBe typeOf<Int>()
//    }
//
//    @Test
//    fun `infer nullability`() {
//        inferNullability(connection)
//    }
//}
