// DUMP_SCHEMAS
import org.jetbrains.kotlinx.dataframe.*
import org.jetbrains.kotlinx.dataframe.annotations.*
import org.jetbrains.kotlinx.dataframe.api.*
import org.jetbrains.kotlinx.dataframe.io.*

interface MySchema {
    val a: String
}

<!SCHEMA!>private fun a()<!> = listOf(1).<!SCHEMA!>toDataFrame<!> {
    "a" from { it }
}

fun test() {
    val <!SCHEMA!>df<!> = <!SCHEMA!>dataFrameOf("a")<!>(123)
    df.<!SCHEMA!>remove<!> { a }
}
