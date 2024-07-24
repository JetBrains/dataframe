package org.jetbrains.dataframe.gradle

import io.kotest.matchers.should
import java.io.File
import java.nio.file.Paths

fun File.shouldEndWith(first: String, vararg path: String) =
    this should { it.endsWith(Paths.get(first, *path).toFile()) }
