package org.jetbrains.kotlinx.dataframe.io

import kotlinx.datetime.LocalDate
import kotlinx.datetime.LocalDateTime
import kotlinx.datetime.LocalTime
import org.apache.commons.csv.CSVFormat
import org.apache.commons.csv.CSVRecord
import org.apache.commons.io.input.BOMInputStream
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.AnyRow
import org.jetbrains.kotlinx.dataframe.DataColumn
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.annotations.Interpretable
import org.jetbrains.kotlinx.dataframe.annotations.OptInRefine
import org.jetbrains.kotlinx.dataframe.annotations.Refine
import org.jetbrains.kotlinx.dataframe.api.ParserOptions
import org.jetbrains.kotlinx.dataframe.api.forEach
import org.jetbrains.kotlinx.dataframe.api.toDataFrame
import org.jetbrains.kotlinx.dataframe.api.tryParse
import org.jetbrains.kotlinx.dataframe.codeGen.DefaultReadCsvMethod
import org.jetbrains.kotlinx.dataframe.codeGen.DefaultReadDfMethod
import org.jetbrains.kotlinx.dataframe.impl.ColumnNameGenerator
import org.jetbrains.kotlinx.dataframe.impl.api.Parsers
import org.jetbrains.kotlinx.dataframe.impl.api.parse
import org.jetbrains.kotlinx.dataframe.values
import java.io.BufferedInputStream
import java.io.BufferedReader
import java.io.File
import java.io.FileInputStream
import java.io.FileWriter
import java.io.IOException
import java.io.InputStream
import java.io.InputStreamReader
import java.io.Reader
import java.io.StringReader
import java.io.StringWriter
import java.math.BigDecimal
import java.net.URL
import java.nio.charset.Charset
import java.util.zip.GZIPInputStream
import kotlin.reflect.KClass
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf

public class CSV(private val delimiter: Char = ',') : SupportedDataFrameFormat {
    override fun readDataFrame(stream: InputStream, header: List<String>): AnyFrame =
        DataFrame.readCSV(stream = stream, delimiter = delimiter, header = header)

    override fun readDataFrame(file: File, header: List<String>): AnyFrame =
        DataFrame.readCSV(file = file, delimiter = delimiter, header = header)

    override fun acceptsExtension(ext: String): Boolean = ext == "csv"

    override fun acceptsSample(sample: SupportedFormatSample): Boolean = true // Extension is enough

    override val testOrder: Int = 20000

    override fun createDefaultReadMethod(pathRepresentation: String?): DefaultReadDfMethod {
        val arguments = MethodArguments().add("delimiter", typeOf<Char>(), "'%L'", delimiter)
        return DefaultReadCsvMethod(pathRepresentation, arguments)
    }
}

public enum class CSVType(public val format: CSVFormat) {
    DEFAULT(
        CSVFormat.DEFAULT.builder()
            .setAllowMissingColumnNames(true)
            .setIgnoreSurroundingSpaces(true)
            .build(),
    ),
    TDF(
        CSVFormat.TDF.builder()
            .setAllowMissingColumnNames(true)
            .build(),
    ),
}

private val defaultCharset = Charsets.UTF_8

internal fun isCompressed(fileOrUrl: String) = listOf("gz", "zip").contains(fileOrUrl.split(".").last())

internal fun isCompressed(file: File) = listOf("gz", "zip").contains(file.extension)

internal fun isCompressed(url: URL) = isCompressed(url.path)

@Refine
@Interpretable("ReadDelimStr")
public fun DataFrame.Companion.readDelimStr(
    text: String,
    delimiter: Char = ',',
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
): DataFrame<*> =
    StringReader(text).use {
        val format = CSVType.DEFAULT.format.builder()
            .setHeader()
            .setDelimiter(delimiter)
            .build()
        readDelim(it, format, colTypes, skipLines, readLines)
    }

public fun DataFrame.Companion.read(
    fileOrUrl: String,
    delimiter: Char,
    header: List<String> = listOf(),
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8,
): DataFrame<*> =
    catchHttpResponse(asURL(fileOrUrl)) {
        readDelim(
            it,
            delimiter,
            header,
            isCompressed(fileOrUrl),
            getCSVType(fileOrUrl),
            colTypes,
            skipLines,
            readLines,
            duplicate,
            charset,
        )
    }

@OptInRefine
@Interpretable("ReadCSV0")
public fun DataFrame.Companion.readCSV(
    fileOrUrl: String,
    delimiter: Char = ',',
    header: List<String> = listOf(),
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8,
    parserOptions: ParserOptions? = null,
): DataFrame<*> =
    catchHttpResponse(asURL(fileOrUrl)) {
        readDelim(
            it,
            delimiter,
            header,
            isCompressed(fileOrUrl),
            CSVType.DEFAULT,
            colTypes,
            skipLines,
            readLines,
            duplicate,
            charset,
            parserOptions,
        )
    }

public fun DataFrame.Companion.readCSV(
    file: File,
    delimiter: Char = ',',
    header: List<String> = listOf(),
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8,
    parserOptions: ParserOptions? = null,
): DataFrame<*> =
    readDelim(
        FileInputStream(file),
        delimiter,
        header,
        isCompressed(file),
        CSVType.DEFAULT,
        colTypes,
        skipLines,
        readLines,
        duplicate,
        charset,
        parserOptions,
    )

public fun DataFrame.Companion.readCSV(
    url: URL,
    delimiter: Char = ',',
    header: List<String> = listOf(),
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8,
    parserOptions: ParserOptions? = null,
): DataFrame<*> =
    readCSV(
        url.openStream(),
        delimiter,
        header,
        isCompressed(url),
        colTypes,
        skipLines,
        readLines,
        duplicate,
        charset,
        parserOptions,
    )

public fun DataFrame.Companion.readCSV(
    stream: InputStream,
    delimiter: Char = ',',
    header: List<String> = listOf(),
    isCompressed: Boolean = false,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = Charsets.UTF_8,
    parserOptions: ParserOptions? = null,
): DataFrame<*> =
    readDelim(
        stream,
        delimiter,
        header,
        isCompressed,
        CSVType.DEFAULT,
        colTypes,
        skipLines,
        readLines,
        duplicate,
        charset,
        parserOptions,
    )

private fun getCSVType(path: String): CSVType =
    when (path.substringAfterLast('.').lowercase()) {
        "csv" -> CSVType.DEFAULT
        "tdf" -> CSVType.TDF
        else -> throw IOException("Unknown file format")
    }

private fun asStream(fileOrUrl: String) =
    if (isURL(fileOrUrl)) {
        URL(fileOrUrl).toURI()
    } else {
        File(fileOrUrl).toURI()
    }.toURL().openStream()

public fun asURL(fileOrUrl: String): URL =
    if (isURL(fileOrUrl)) {
        URL(fileOrUrl).toURI()
    } else {
        File(fileOrUrl).toURI()
    }.toURL()

private fun getFormat(
    type: CSVType,
    delimiter: Char,
    header: List<String>,
    duplicate: Boolean,
): CSVFormat =
    type.format.builder()
        .setDelimiter(delimiter)
        .setHeader(*header.toTypedArray())
        .setAllowMissingColumnNames(duplicate)
        .build()

public fun DataFrame.Companion.readDelim(
    inStream: InputStream,
    delimiter: Char = ',',
    header: List<String> = listOf(),
    isCompressed: Boolean = false,
    csvType: CSVType,
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    duplicate: Boolean = true,
    charset: Charset = defaultCharset,
    parserOptions: ParserOptions? = null,
): AnyFrame {
    val bufferedInStream = BufferedInputStream(if (isCompressed) GZIPInputStream(inStream) else inStream)
    val bomIn = BOMInputStream.builder().setInputStream(bufferedInStream).get()
    val bufferedReader = BufferedReader(InputStreamReader(bomIn, charset))

    return readDelim(
        reader = bufferedReader,
        format = getFormat(csvType, delimiter, header, duplicate),
        colTypes = colTypes,
        skipLines = skipLines,
        readLines = readLines,
        parserOptions = parserOptions,
    )
}

public enum class ColType {
    Int,
    Long,
    Double,
    Boolean,
    BigDecimal,
    LocalDate,
    LocalTime,
    LocalDateTime,
    String,
}

public fun ColType.toType(): KClass<out Any> =
    when (this) {
        ColType.Int -> Int::class
        ColType.Long -> Long::class
        ColType.Double -> Double::class
        ColType.Boolean -> Boolean::class
        ColType.BigDecimal -> BigDecimal::class
        ColType.LocalDate -> LocalDate::class
        ColType.LocalTime -> LocalTime::class
        ColType.LocalDateTime -> LocalDateTime::class
        ColType.String -> String::class
    }

public fun DataFrame.Companion.readDelim(
    reader: Reader,
    format: CSVFormat = CSVFormat.DEFAULT.builder()
        .setHeader()
        .build(),
    colTypes: Map<String, ColType> = mapOf(),
    skipLines: Int = 0,
    readLines: Int? = null,
    parserOptions: ParserOptions? = null,
): AnyFrame {
    var reader = reader
    if (skipLines > 0) {
        reader = BufferedReader(reader)
        repeat(skipLines) { reader.readLine() }
    }

    val csvParser = format.parse(reader)
    val records = if (readLines == null) {
        csvParser.records
    } else {
        require(readLines >= 0) { "`readLines` must not be negative" }
        val records = ArrayList<CSVRecord>(readLines)
        val iter = csvParser.iterator()
        var count = readLines ?: 0
        while (iter.hasNext() && 0 < count--) {
            records.add(iter.next())
        }
        records
    }

    val columnNames = csvParser.headerNames.takeIf { it.isNotEmpty() }
        ?: (1..(records.firstOrNull()?.count() ?: 0)).map { index -> "X$index" }

    val generator = ColumnNameGenerator()
    val uniqueNames = columnNames.map { generator.addUnique(it) }

    val cols = uniqueNames.mapIndexed { colIndex, colName ->
        val defaultColType = colTypes[".default"]
        val colType = colTypes[colName] ?: defaultColType
        var hasNulls = false
        val values = records.map {
            if (it.isSet(colIndex)) {
                it[colIndex].ifEmpty {
                    hasNulls = true
                    null
                }
            } else {
                hasNulls = true
                null
            }
        }
        val column = DataColumn.createValueColumn(colName, values, typeOf<String>().withNullability(hasNulls))
        when (colType) {
            null -> column.tryParse(parserOptions)

            else -> {
                val parser = Parsers[colType.toType()]!!
                column.parse(parser, parserOptions)
            }
        }
    }
    return cols.toDataFrame()
}

public fun AnyFrame.writeCSV(file: File, format: CSVFormat = CSVFormat.DEFAULT): Unit =
    writeCSV(FileWriter(file), format)

public fun AnyFrame.writeCSV(path: String, format: CSVFormat = CSVFormat.DEFAULT): Unit =
    writeCSV(FileWriter(path), format)

public fun AnyFrame.writeCSV(writer: Appendable, format: CSVFormat = CSVFormat.DEFAULT) {
    format.print(writer).use { printer ->
        if (!format.skipHeaderRecord) {
            printer.printRecord(columnNames())
        }
        forEach {
            val values = it.values.map {
                when (it) {
                    is AnyRow -> it.toJson()
                    is AnyFrame -> it.toJson()
                    else -> it
                }
            }
            printer.printRecord(values)
        }
    }
}

public fun AnyFrame.toCsv(format: CSVFormat = CSVFormat.DEFAULT): String =
    StringWriter().use {
        this.writeCSV(it, format)
        it
    }.toString()
