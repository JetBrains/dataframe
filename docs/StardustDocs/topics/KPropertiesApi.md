[//]: # (title: KProperties API)

<!---IMPORT org.jetbrains.kotlinx.dataframe.samples.api.ApiLevels-->

[`DataFrame`](DataFrame.md) can be used as an intermediate structure for data transformation between two data formats. 
If either source or destination is a Kotlin object, e.g. data class, it is convenient to use its properties 
for typed data access in [`DataFrame`](DataFrame.md).
This can be done using `::` expression that provides [property references](https://kotlinlang.org/api/latest/jvm/stdlib/kotlin.reflect/-k-property/)

<!---FUN kproperties1-->

```kotlin
data class Passenger(
    val survived: Boolean,
    val home: String,
    val age: Int?,
    val lastName: String
)

val passengers = DataFrame.read("titanic.csv")
    .add(Passenger::lastName) { "name"<String>().split(",").last() }
    .dropNulls(Passenger::age)
    .filter {
        it[Passenger::survived] &&
            it[Passenger::home].endsWith("NY") &&
            it[Passenger::age] in 10..20
    }
    .toListOf<Passenger>()
```

<!---END-->

By default, [`DataFrame`](DataFrame.md) uses `name` and `returnType` of `KProperty` for typed access to data. 
When column name differs from property name, use `@ColumnName` annotation:  

<!---FUN kproperties2-->

```kotlin
data class Passenger(
    @ColumnName("survived") val isAlive: Boolean,
    @ColumnName("home") val city: String,
    val name: String
)

val passengers = DataFrame.read("titanic.csv")
    .filter { it[Passenger::city].endsWith("NY") }
    .toListOf<Passenger>()
```

<!---END-->

The `titanic.csv` file could be found [here](https://github.com/Kotlin/dataframe/blob/master/data/titanic.csv).
