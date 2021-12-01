package org.jetbrains.dataframe.gradle

import com.beust.klaxon.KlaxonException
import io.kotest.assertions.asClue
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.io.read
import org.junit.Test
import java.io.File
import java.io.FileNotFoundException
import java.io.IOException
import java.net.URL
import java.nio.file.Files

class DataFrameReadTest {

    @Test
    fun `file that does not exists`() {
        val temp = Files.createTempDirectory("").toFile()
        val definitelyDoesNotExists = File(temp, "absolutelyRandomName")
        shouldThrow<FileNotFoundException> {
            DataFrame.read(definitelyDoesNotExists)
        }
    }

    @Test
    fun `file with invalid json`() {
        val temp = Files.createTempDirectory("").toFile()
        val invalidJson = File(temp, "test.json").also { it.writeText(".") }
        shouldThrow<KlaxonException> {
            DataFrame.read(invalidJson)
        }
    }

    @Test
    fun `file with invalid csv`() {
        val temp = Files.createTempDirectory("").toFile()
        val invalidCsv = File(temp, "test.csv").also { it.writeText("") }
        shouldThrow<IndexOutOfBoundsException> {
            DataFrame.read(invalidCsv)
        }
    }

    @Test
    fun `invalid url`() {
        val exception = shouldThrowAny {
            DataFrame.read("http:://example.com")
        }
        exception.asClue {
            (exception is IllegalArgumentException || exception is IOException) shouldBe true
        }
    }

    @Test
    fun `valid url`() {
        val df = DataFrame.read(URL("http://example.com"))
        println(df)
    }

    @Test
    fun `path that is valid url`() {
        val df = DataFrame.read("http://example.com")
        println(df)
    }

    @Test
    fun `URL with invalid JSON`() {
        shouldThrow<KlaxonException> {
            DataFrame.read("https://github.com/Kotlin/dataframe/blob/8ea139c35aaf2247614bb227756d6fdba7359f6a/data/playlistItems.json")
        }
    }

    @Test
    fun `data accessible and readable`() {
        val df = DataFrame.read(File("../../data/jetbrains_repositories.csv"))
        val df1 = DataFrame.read(File("../../data/playlistItems.json"))
    }

    @Test
    fun `csvSample is valid csv`() {
        val temp = Files.createTempFile("f", "csv").toFile()
        temp.writeText(TestData.csvSample)

        val df = DataFrame.read(temp)
        df.columnNames() shouldBe listOf("name", "age")
    }
}
