package org.jetbrains.kotlinx.dataframe.api

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.alsoDebug
import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.columns.ColumnKind
import org.jetbrains.kotlinx.dataframe.exceptions.TypeConverterNotFoundException
import org.jetbrains.kotlinx.dataframe.kind
import org.junit.Test
import kotlin.reflect.typeOf

@Suppress("ktlint:standard:argument-list-wrapping")
class ConvertToTests {

    @Test
    fun `convert frame column with empty frames`() {
        val groups by columnOf(dataFrameOf("a")("1"), DataFrame.empty())
        val df = dataFrameOf(groups)

        @DataSchema
        data class GroupSchema(val a: Int)

        @DataSchema
        data class DataFrameSchema(val groups: DataFrame<GroupSchema>)

        val converted = df.convertTo<DataFrameSchema>()

        converted[groups].forEach {
            it["a"].type() shouldBe typeOf<Int>()
        }
    }

    data class A(val value: Int)

    @DataSchema
    data class Schema(val a: A)

    @Test
    fun `convert with parser`() {
        val df = dataFrameOf("a")("1")

        shouldThrow<TypeConverterNotFoundException> {
            df.convertTo<Schema>()
        }

        df.convertTo<Schema> { parser { A(it.toInt()) } }
            .single()
            .a.value shouldBe 1

        // shortcut for:
        df.convertTo<Schema> { convert<String>().with { A(it.toInt()) } }
            .single()
            .a.value shouldBe 1
    }

    @Test
    fun `convert from char with parser`() {
        val df = dataFrameOf("a")('1')

        shouldThrow<TypeConverterNotFoundException> {
            df.convertTo<Schema>()
        }

        // Char -> String -> Target
        df.convertTo<Schema> { parser { A(it.toInt()) } }
            .single()
            .a.value shouldBe 1

        // shortcut for:
        df.convertTo<Schema> { convert<String>().with { A(it.toInt()) } }
            .single()
            .a.value shouldBe 1

        // Char -> Target
        df.convertTo<Schema> {
            parser<A> { error("should not be triggered if convert<Char>() is present") }
            convert<String>().with<_, A> { error("should not be triggered if convert<Char>() is present") }

            convert<Char>().with { A(it.digitToInt()) }
        }.single().a.value shouldBe 1
    }

    @Test
    fun `convert with converter`() {
        val df = dataFrameOf("a")(1)

        shouldThrow<TypeConverterNotFoundException> {
            df.convertTo<Schema>()
        }

        df.convertTo<Schema> { convert<Int>().with { A(it) } }
            .single()
            .a.value shouldBe 1
    }

    @Test
    fun `convert nulls to not nulls with converter`() {
        val df = dataFrameOf("a")("1", null)

        val converted = df.convertTo<Schema> {
            convert<String?>().with { it?.let { A(it.toInt()) } ?: A(0) }
        }
        val expected = dataFrameOf("a")(A(1), A(0))

        converted shouldBe expected
    }

    @JvmInline
    value class IntClass(val value: Int)

    @DataSchema
    interface IntSchema {
        val a: IntClass?
    }

    @Test
    fun `convert value class with converter`() {
        dataFrameOf("a")("1%")
            .convertTo<IntSchema> {
                parser { IntClass(it.dropLast(1).toInt()) }
            } shouldBe dataFrameOf("a")(IntClass(1))
    }

    @Test
    fun `convert nulls with converter`() {
        dataFrameOf("a")("1%", null)
            .convertTo<IntSchema> {
                parser { IntClass(it.dropLast(1).toInt()) }
            } shouldBe dataFrameOf("a")(IntClass(1), null)
    }

    @Test
    fun `convert with nullable converter argument`() {
        val df = dataFrameOf("a")("1")

        val converted = df.convertTo<IntSchema> {
            convert<String?>().with {
                it?.let { IntClass(it.toInt()) }
            }
        }
        val expected = dataFrameOf("a")(IntClass(1))

        converted shouldBe expected
    }

    @DataSchema
    data class Location(val name: String, val gps: Gps?)

    @DataSchema
    data class Gps(val latitude: Double, val longitude: Double)

    // @Test TODO: https://github.com/Kotlin/dataframe/issues/177
    fun `convert df with nullable DataRow`() {
        val locations: AnyFrame = dataFrameOf("name", "gps")(
            "Home", Gps(0.0, 0.0),
            "Away", null,
        )

        locations.print(borders = true, title = true, columnTypes = true)
        locations.schema().print()

        val converted = locations.convertTo<Location>()

        converted shouldBe locations
    }

    @Test
    fun `convert df with nullable DataRow to itself`() {
        val locations: DataFrame<Location> = listOf(
            Location("Home", Gps(0.0, 0.0)),
            Location("Away", null),
        ).toDataFrame()

        val converted = locations.convertTo<Location>()

        converted shouldBe locations
    }

    @DataSchema
    data class DataSchemaWithAnyFrame(val dfs: AnyFrame?)

    @Test
    fun test() {
        val df1 = dataFrameOf("a")(1, 2, 3)
        val df2 = dataFrameOf("b")(4, 5)
        val frameColumn by columnOf(df1, df2, null)
        val df = dataFrameOf(frameColumn).alsoDebug()
//        ⌌---------------⌍
//        |  | untitled:[]|
//        |--|------------|
//        | 0|     [3 x 1]|
//        | 1|     [2 x 1]|
//        | 2|     [0 x 0]|
//        ⌎---------------⌏
//
//        untitled: *
    }

    @Test
    fun `convert df with AnyFrame to itself`() {
        val locationsList = listOf(
            Location("Home", Gps(0.0, 0.0)),
            Location("Away", null),
            null,
        )
        val locations = locationsList
            .toDataFrame()
            .alsoDebug("locations:")

        val gpsList = listOf(
            Gps(0.0, 0.0),
            null,
        )
        val gps = gpsList
            .toDataFrame()
            .alsoDebug("gps:")

        val df1 = listOf(
            DataSchemaWithAnyFrame(locations),
        ).toDataFrame()
            .alsoDebug("df1:")

        df1.convertTo<DataSchemaWithAnyFrame>()

        val df2 = listOf(
            DataSchemaWithAnyFrame(gps),
        ).toDataFrame()
            .alsoDebug("df2:")

        df2.convertTo<DataSchemaWithAnyFrame>()

        val df3 = listOf(
            DataSchemaWithAnyFrame(null),
            DataSchemaWithAnyFrame(gps),
        ).toDataFrame { properties { preserve(DataFrame::class) } }
            .alsoDebug("df3 before convert:")

        df3.convertTo<DataSchemaWithAnyFrame>()

        val df4 = listOf(
            DataSchemaWithAnyFrame(null),
        ).toDataFrame { properties { preserve(DataFrame::class) } }
            .alsoDebug("df4 before convert:")

        df4.convertTo<DataSchemaWithAnyFrame>()

        val df5a: DataFrame<*> = dataFrameOf(
            columnOf(locations, gps, null).named("dfs"),
        ).alsoDebug("df5a:")

        df5a.convertTo<DataSchemaWithAnyFrame>()

        val df5 = listOf(
            DataSchemaWithAnyFrame(null),
            DataSchemaWithAnyFrame(locations),
            DataSchemaWithAnyFrame(gps),
        ).toDataFrame { properties { preserve(DataFrame::class) } }
            .alsoDebug("df5 before convert:")

        df5.convertTo<DataSchemaWithAnyFrame>()
            .alsoDebug("df5 after convert:")
            .convertTo<DataSchemaWithAnyFrame>()
            .alsoDebug("df5 after second convert:")
    }

    interface KeyValue<T> {
        val key: String
        val value: T
    }

    @DataSchema
    interface MySchema : KeyValue<Int>

    @Test
    fun `Convert generic interface to itself`() {
        val df = dataFrameOf("key", "value")(
            "a", 1,
            "b", 2,
        ).alsoDebug()
        val converted = df.convertTo<MySchema>().alsoDebug()
        converted shouldBe df
    }

    @Test
    fun `convert with missing nullable column`() {
        @DataSchema
        data class Result(val a: Int, val b: Int?)

        val df = dataFrameOf("a")(1, 2)
        val converted = df.convertTo<Result>()
        converted shouldBe listOf(Result(1, null), Result(2, null)).toDataFrame()
    }

    @Test
    fun `convert with custom fill of missing columns`() {
        val locations = listOf(
            Location("Home", Gps(1.0, 1.0)),
            Location("Away", null),
        ).toDataFrame().cast<Location>()

        val converted = locations
            .remove { gps.longitude }
            .cast<Unit>()
            .convertTo<Location> {
                fill { gps.longitude }.with { gps.latitude }
            }

        converted shouldBe locations.update { gps.longitude }.with { gps.latitude }
    }

    @Test
    fun `convert column of empty lists into FrameColumn`() {
        @DataSchema
        data class Entry(val v: Int)

        @DataSchema
        data class Result(val d: DataFrame<Entry>)

        dataFrameOf("d")(emptyList<Any>(), emptyList<Any>())
            .convertTo<Result>() shouldBe
            dataFrameOf("d")(DataFrame.emptyOf<Entry>(), DataFrame.emptyOf<Entry>())
    }

    @Test
    fun `convert ColumnGroup into FrameColumn`() {
        @DataSchema
        data class Entry(val v: Int)

        @DataSchema
        data class Result(val d: DataFrame<Entry>)

        val columnGroup = DataColumn.createColumnGroup("d", dataFrameOf("v")(1, 2))
        columnGroup.kind() shouldBe ColumnKind.Group
        val res = dataFrameOf(columnGroup).convertTo<Result>()
        val frameColumn = res.getFrameColumn("d")
        frameColumn.kind shouldBe ColumnKind.Frame
        frameColumn.values() shouldBe listOf(dataFrameOf("v")(1), dataFrameOf("v")(2))
    }

    @Test
    fun `convert ValueColumn of lists, nulls and frames into FrameColumn`() {
        @DataSchema
        data class Entry(val v: Int)

        @DataSchema
        data class Result(val d: DataFrame<Entry>)

        val emptyList: List<Any?> = emptyList()
        val listOfRows: List<AnyRow> = dataFrameOf("v")(1, 2).rows().toList()
        val frame: DataFrame<Entry> = listOf(Entry(3), Entry(4)).toDataFrame()

        val src = DataColumn.createValueColumn("d", listOf(emptyList, listOfRows, frame, null)).toDataFrame()
        src["d"].kind shouldBe ColumnKind.Value

        val df = src.convertTo<Result>()
        val frameColumn = df.getFrameColumn("d")
        frameColumn.kind shouldBe ColumnKind.Frame
        frameColumn.toList() shouldBe listOf(
            DataFrame.emptyOf<Entry>(),
            dataFrameOf("v")(1, 2),
            dataFrameOf("v")(3, 4),
            DataFrame.emptyOf<Entry>(),
        )
    }
}
