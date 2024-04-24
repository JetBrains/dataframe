package org.jetbrains.kotlinx.dataframe.io

import org.apache.arrow.dataset.file.FileFormat
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.ArrowReader
import org.apache.commons.compress.utils.SeekableInMemoryByteChannel
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.NullabilityOptions
import org.jetbrains.kotlinx.dataframe.codeGen.AbstractDefaultReadMethod
import org.jetbrains.kotlinx.dataframe.codeGen.DefaultReadDfMethod
import java.io.File
import java.io.InputStream
import java.net.URL
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel
import java.nio.channels.SeekableByteChannel
import java.nio.file.Files

public class ArrowFeather : SupportedDataFrameFormat {
    override fun readDataFrame(stream: InputStream, header: List<String>): AnyFrame =
        DataFrame.readArrowFeather(stream, NullabilityOptions.Widening)

    override fun readDataFrame(file: File, header: List<String>): AnyFrame =
        DataFrame.readArrowFeather(file, NullabilityOptions.Widening)

    override fun acceptsExtension(ext: String): Boolean = ext == "feather"

    override fun acceptsSample(sample: SupportedFormatSample): Boolean = true // Extension is enough

    override val testOrder: Int = 50000

    override fun createDefaultReadMethod(pathRepresentation: String?): DefaultReadDfMethod =
        DefaultReadArrowMethod(pathRepresentation)
}

private const val READ_ARROW_FEATHER = "readArrowFeather"

private class DefaultReadArrowMethod(path: String?) :
    AbstractDefaultReadMethod(path, MethodArguments.EMPTY, READ_ARROW_FEATHER)

internal object Allocator {
    val ROOT by lazy {
        RootAllocator(Long.MAX_VALUE)
    }
}

/**
 * Read [Arrow interprocess streaming format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format) data from existing [channel]
 */
public fun DataFrame.Companion.readArrowIPC(
    channel: ReadableByteChannel,
    allocator: RootAllocator = Allocator.ROOT,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = readArrowIPCImpl(channel, allocator, nullability)

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [channel]
 */
public fun DataFrame.Companion.readArrowFeather(
    channel: SeekableByteChannel,
    allocator: RootAllocator = Allocator.ROOT,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = readArrowFeatherImpl(channel, allocator, nullability)

// IPC reading block

/**
 * Read [Arrow interprocess streaming format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format) data from existing [file]
 */
public fun DataFrame.Companion.readArrowIPC(
    file: File,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = Files.newByteChannel(file.toPath()).use { readArrowIPC(it, nullability = nullability) }

/**
 * Read [Arrow interprocess streaming format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format) data from existing [byteArray]
 */
public fun DataFrame.Companion.readArrowIPC(
    byteArray: ByteArray,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = SeekableInMemoryByteChannel(byteArray).use { readArrowIPC(it, nullability = nullability) }

/**
 * Read [Arrow interprocess streaming format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format) data from existing [stream]
 */
public fun DataFrame.Companion.readArrowIPC(
    stream: InputStream,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = Channels.newChannel(stream).use { readArrowIPC(it, nullability = nullability) }

/**
 * Read [Arrow interprocess streaming format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-streaming-format) data from existing [url]
 */
public fun DataFrame.Companion.readArrowIPC(
    url: URL,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame =
    when {
        isFile(url) -> readArrowIPC(urlAsFile(url), nullability)

        isProtocolSupported(url) -> url.openStream().use { readArrowIPC(it, nullability) }

        else -> {
            throw IllegalArgumentException("Invalid protocol for url $url")
        }
    }

public fun DataFrame.Companion.readArrowIPC(
    path: String,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame =
    if (isUrl(path)) {
        readArrowIPC(URL(path), nullability)
    } else {
        readArrowIPC(File(path), nullability)
    }

// Feather reading block

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [file]
 */
public fun DataFrame.Companion.readArrowFeather(
    file: File,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = Files.newByteChannel(file.toPath()).use { readArrowFeather(it, nullability = nullability) }

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [byteArray]
 */
public fun DataFrame.Companion.readArrowFeather(
    byteArray: ByteArray,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = SeekableInMemoryByteChannel(byteArray).use { readArrowFeather(it, nullability = nullability) }

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [stream]
 */
public fun DataFrame.Companion.readArrowFeather(
    stream: InputStream,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = readArrowFeather(stream.readBytes(), nullability)

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [url]
 */
public fun DataFrame.Companion.readArrowFeather(
    url: URL,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame =
    when {
        isFile(url) -> readArrowFeather(urlAsFile(url), nullability)

        isProtocolSupported(url) -> readArrowFeather(url.readBytes(), nullability)

        else -> {
            throw IllegalArgumentException("Invalid protocol for url $url")
        }
    }

/**
 * Read [Arrow random access format](https://arrow.apache.org/docs/java/ipc.html#writing-and-reading-random-access-files) data from existing [path]
 */
public fun DataFrame.Companion.readArrowFeather(
    path: String,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame =
    if (isUrl(path)) {
        readArrowFeather(URL(path), nullability)
    } else {
        readArrowFeather(File(path), nullability)
    }

/**
 * Read [Arrow any format](https://arrow.apache.org/docs/java/ipc.html#reading-writing-ipc-formats) data from existing [reader]
 */
public fun DataFrame.Companion.readArrow(
    reader: ArrowReader,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = readArrowImpl(reader, nullability)

/**
 * Read [Arrow any format](https://arrow.apache.org/docs/java/ipc.html#reading-writing-ipc-formats) data from existing [ArrowReader]
 */
public fun ArrowReader.toDataFrame(nullability: NullabilityOptions = NullabilityOptions.Infer): AnyFrame =
    DataFrame.Companion.readArrowImpl(this, nullability)

/**
 * Read [Parquet](https://parquet.apache.org/) data from existing [url] by using [Arrow Dataset](https://arrow.apache.org/docs/java/dataset.html)
 */
public fun DataFrame.Companion.readParquet(
    url: URL,
    nullability: NullabilityOptions = NullabilityOptions.Infer,
): AnyFrame = readArrowDataset(url.toString(), FileFormat.PARQUET, nullability)
