[//]: # (title: Interop with Collections)

<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.Access-->
<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.Collections-->

_Kotlin DataFrame_ and _Kotlin Collection_ represent two different approaches to data storage:
* [`DataFrame`](DataFrame.md) stores data by fields/columns
* `Collection` stores data by records/rows

Although [`DataFrame`](DataFrame.md)
doesn't implement the [`Collection`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-collection/#kotlin.collections.Collection)
or [`Iterable`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-iterable/)
interface, it has many similar operations,
such as [`filter`](filter.md), [`take`](sliceRows.md#take),
[`first`](first.md), [`map`](map.md), [`groupBy`](groupBy.md) etc.

[`DataFrame`](DataFrame.md) has two-way compatibility with [`Map`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-map/) and [`List`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-list/):
* `List<T>` -> `DataFrame<T>`: [toDataFrame](createDataFrame.md#todataframe)
* `DataFrame<T>` -> `List<T>`: [toList](toList.md)
* `Map<String, List<*>>` -> `DataFrame<*>`: [toDataFrame](createDataFrame.md#todataframe)
* `DataFrame<*>` -> `Map<String, List<*>>`: [toMap](toMap.md)

Columns, rows, and values of [`DataFrame`](DataFrame.md)
can be accessed as [`List`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-list/),
[`Iterable`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-iterable/)
and [`Sequence`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.sequences/-sequence/) accordingly:

<!---FUN getRowsColumns-->

```kotlin
df.columns() // List<DataColumn>
df.rows() // Iterable<DataRow>
df.values() // Sequence<Any?>
```

<!---END-->

## Interop with data classes

[`DataFrame`](DataFrame.md) can be used as an intermediate object for transformation from one data structure to another.

Assume you have a list of instances of some [data class](https://kotlinlang.org/docs/data-classes.html) that you need to transform into some other format.

<!---FUN listInterop1-->

```kotlin
data class Input(val a: Int, val b: Int)

val list = listOf(Input(1, 2), Input(3, 4))
```

<!---END-->

You can convert this list into [`DataFrame`](DataFrame.md) using [`toDataFrame()`](createDataFrame.md#todataframe) extension:

<!---FUN listInterop2-->

```kotlin
val df = list.toDataFrame()
```

<!---END-->

Mark the original data class with [`DataSchema`](schemas.md)
annotation to get [extension properties](extensionPropertiesApi.md) and perform data transformations.

<!---FUN listInterop3-->

```kotlin
@DataSchema
data class Input(val a: Int, val b: Int)

val df2 = df.add("c") { a + b }
```

<!---END-->

<tip>

To enable extension properties generation, you should use the [DataFrame plugin](schemasGradle.md) 
for Gradle or the [Kotlin Jupyter kernel](gettingStartedJupyterNotebook.md)

</tip>

After your data is transformed, [`DataFrame`](DataFrame.md) instances can be exported eagerly
into [`List`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-list/) of another data class using [toList](toList.md) or [toListOf](toList.md#tolistof) extensions:

<!---FUN listInterop4-->

```kotlin
data class Output(val a: Int, val b: Int, val c: Int)

val result = df2.toListOf<Output>()
```

Alternatively, one can create lazy [`Sequence`](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.collections/-sequence/) objects.
This avoids holding the entire list of objects in memory as objects are created on the fly as needed.

<!---FUN listInterop5-->

```kotlin
data class Output(val a: Int, val b: Int, val c: Int)

val result = df2.toSequenceOf<Output>()
```

<!---END-->

### Converting columns with object instances to ColumnGroup

[unfold](unfold.md) can be used as [`toDataFrame()`](createDataFrame.md#todataframe) analogue for specific columns inside existing dataframes
