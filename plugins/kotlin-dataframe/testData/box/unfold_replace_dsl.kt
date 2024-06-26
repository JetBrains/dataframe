import org.jetbrains.kotlinx.dataframe.*
import org.jetbrains.kotlinx.dataframe.api.*

data class Function(val name: String)

data class Declaration(val name: String, val functions: Function)

fun box(): String {
    val df = listOf(Declaration("DataFrameImpl", Function("rowsCount")))
        .toDataFrame()

    val a: String = df.name[0]
    val b: Function = df.functions[0]

    val df1 = df.replace { functions }.unfold {
        properties(maxDepth = 0)
        "nameLength" from { it.name.length }
    }
    val c: DataColumn<String> = df1.functions.name
    val d: DataColumn<Int> = df1.functions.nameLength

    return "OK"
}
