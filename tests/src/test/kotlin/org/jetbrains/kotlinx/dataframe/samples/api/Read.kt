@file:Suppress("ktlint")

package org.jetbrains.kotlinx.dataframe.samples.api

import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.DataRow
import org.jetbrains.kotlinx.dataframe.api.ParserOptions
import org.jetbrains.kotlinx.dataframe.api.columnNames
import org.jetbrains.kotlinx.dataframe.api.columnTypes
import org.jetbrains.kotlinx.dataframe.io.ColType
import org.jetbrains.kotlinx.dataframe.io.StringColumns
import org.jetbrains.kotlinx.dataframe.io.readArrowFeather
import org.jetbrains.kotlinx.dataframe.io.readCSV
import org.jetbrains.kotlinx.dataframe.io.readExcel
import org.jetbrains.kotlinx.dataframe.io.readJson
import org.jetbrains.kotlinx.dataframe.testArrowFeather
import org.jetbrains.kotlinx.dataframe.testCsv
import org.jetbrains.kotlinx.dataframe.testJson
import org.junit.Ignore
import org.junit.Test
import java.util.Locale
import kotlin.reflect.typeOf

class Read {
    @Test
    fun readCsvCustom() {
        val file = testCsv("syntheticSample")
        // SampleStart
        val df = DataFrame.readCSV(
            file,
            delimiter = '|',
            header = listOf("A", "B", "C", "D"),
            parserOptions = ParserOptions(nullStrings = setOf("not assigned")),
        )
        // SampleEnd
        df.rowsCount() shouldBe 3
        df.columnNames() shouldBe listOf("A", "B", "C", "D")
        df["A"].type() shouldBe typeOf<Int>()
        df["D"].type() shouldBe typeOf<Boolean?>()
    }

    @Test
    fun readJson() {
        val file = testJson("synthetic")
        // SampleStart
        val df = DataFrame.readJson(file)
        // SampleEnd
        df.rowsCount() shouldBe 4
        df.columnNames() shouldBe listOf("A", "B", "C", "D")
        df["A"].type() shouldBe typeOf<String>()
        df["B"].type() shouldBe typeOf<Int>()
        df["D"].type() shouldBe typeOf<Boolean?>()
    }

    @Test
    fun readJsonRow() {
        val file = testJson("syntheticObj")
        // SampleStart
        val row = DataRow.readJson(file)
        // SampleEnd
        row.columnNames() shouldBe listOf("A", "B", "C", "D")
        row.columnTypes() shouldBe listOf(typeOf<String>(), typeOf<Int>(), typeOf<Double>(), typeOf<Boolean>())
    }

    @Test
    @Ignore
    fun fixMixedColumn() {
        // SampleStart
        val df = DataFrame.readExcel("mixed_column.xlsx", stringColumns = StringColumns("A"))
        // SampleEnd
    }

    @Test
    fun readArrowFeather() {
        val file = testArrowFeather("data-arrow_2.0.0_uncompressed")
        // SampleStart
        val df = DataFrame.readArrowFeather(file)
        // SampleEnd
        df.rowsCount() shouldBe 1
        df.columnsCount() shouldBe 4
    }

    @Test
    fun readNumbersWithSpecificLocale() {
        val file = testCsv("numbers")
        // SampleStart
        val df = DataFrame.readCSV(
            file,
            parserOptions = ParserOptions(locale = Locale.UK),
        )
        // SampleEnd
    }

    @Test
    fun readNumbersWithColType() {
        val file = testCsv("numbers")
        // SampleStart
        val df = DataFrame.readCSV(
            file,
            colTypes = mapOf("colName" to ColType.String),
        )
        // SampleEnd
    }
}
