package org.jetbrains.kotlinx.dataframe.io

import kotlinx.datetime.Instant
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
import org.jetbrains.kotlinx.dataframe.DataRow
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
import org.jetbrains.kotlinx.dataframe.impl.api.parse
import org.jetbrains.kotlinx.dataframe.util.AS_URL
import org.jetbrains.kotlinx.dataframe.util.AS_URL_IMPORT
import org.jetbrains.kotlinx.dataframe.util.AS_URL_REPLACE
import org.jetbrains.kotlinx.dataframe.util.DF_READ_NO_CSV
import org.jetbrains.kotlinx.dataframe.util.DF_READ_NO_CSV_REPLACE
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
import kotlin.reflect.KType
import kotlin.reflect.full.withNullability
import kotlin.reflect.typeOf
import kotlin.time.Duration

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

@Deprecated(
    message = DF_READ_NO_CSV,
    replaceWith = ReplaceWith(DF_READ_NO_CSV_REPLACE),
    level = DeprecationLevel.WARNING,
)
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
    catchHttpResponse(asUrl(fileOrUrl)) {
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
    catchHttpResponse(asUrl(fileOrUrl)) {
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

@Deprecated(
    message = AS_URL,
    replaceWith = ReplaceWith(AS_URL_REPLACE, AS_URL_IMPORT),
    level = DeprecationLevel.WARNING,
)
public fun asURL(fileOrUrl: String): URL = asUrl(fileOrUrl)

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

/** Column types that DataFrame can [parse] from a [String]. */
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
    Instant,
    Duration,
    Url,
    JsonArray,
    JsonObject,
    Char,
    ;

    public companion object {

        /**
         * You can add a default column type to the `colTypes` parameter
         * by setting the key to [ColType.DEFAULT] and the value to the desired type.
         */
        public const val DEFAULT: kotlin.String = ".default"
    }
}

public fun ColType.toType(): KClass<*> = toKType().classifier as KClass<*>

public fun ColType.toKType(): KType =
    when (this) {
        ColType.Int -> typeOf<Int>()
        ColType.Long -> typeOf<Long>()
        ColType.Double -> typeOf<Double>()
        ColType.Boolean -> typeOf<Boolean>()
        ColType.BigDecimal -> typeOf<BigDecimal>()
        ColType.LocalDate -> typeOf<LocalDate>()
        ColType.LocalTime -> typeOf<LocalTime>()
        ColType.LocalDateTime -> typeOf<LocalDateTime>()
        ColType.String -> typeOf<String>()
        ColType.Instant -> typeOf<Instant>()
        ColType.Duration -> typeOf<Duration>()
        ColType.Url -> typeOf<URL>()
        ColType.JsonArray -> typeOf<DataFrame<*>>()
        ColType.JsonObject -> typeOf<DataRow<*>>()
        ColType.Char -> typeOf<Char>()
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
    try {
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
            val skipTypes = when {
                colType != null ->
                    // skip all types except the desired type
                    ParserOptions.allTypesExcept(colType.toKType())

                else ->
                    // respect the provided parser options
                    parserOptions?.skipTypes ?: emptySet()
            }
            val adjustsedParserOptions = (parserOptions ?: ParserOptions())
                .copy(skipTypes = skipTypes)

            return@mapIndexed column.tryParse(adjustsedParserOptions)
        }
        return cols.toDataFrame()
    } catch (e: OutOfMemoryError) {
        throw OutOfMemoryError(
            "Ran out of memory reading this CSV-like file. " +
                "You can try our new experimental CSV reader by adding the dependency " +
                "\"org.jetbrains.kotlinx:dataframe-csv:{VERSION}\" and using `DataFrame.readCsv()` instead of " +
                "`DataFrame.readCSV()`. This requires `@OptIn(ExperimentalCsv::class)`.",
        )
    }
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
