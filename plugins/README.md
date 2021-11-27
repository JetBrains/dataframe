# DataFrame Gradle integration and annotation processing

DataFrame Gradle plugin can
1. Generate type-safe accessors for your data for interfaces annotated with `@DataSchema` - **data schemas**
2. Infer data schemas from your CSV or JSON data.

## Setup

### build.gradle.kts
```
plugins {
    id("org.jetbrains.kotlin.plugin.dataframe") version "$DATAFRAME_VERSION"
}

dependencies {
    implementation("org.jetbrains.kotlinx:dataframe:$DATAFRAME_VERSION")
}

// Make IDE aware of the generated code:
kotlin.sourceSets.getByName("main").kotlin.srcDir("build/generated/ksp/main/kotlin/")

// Excludes for `kotlint`:
tasks.withType<org.jmailen.gradle.kotlinter.tasks.LintTask> {
    exclude {
        it.name.endsWith(".Generated.kt")
    }
    exclude {
        it.name.endsWith("\$Extensions.kt")
    }
}
```

### build.gradle
```
plugins {
    id("org.jetbrains.kotlin.plugin.dataframe") version "$DATAFRAME_VERSION"
}

dependencies {
    implementation("org.jetbrains.kotlinx:dataframe:$DATAFRAME_VERSION")
}

// Make IDE aware of the generated code:
sourceSets {
    main.kotlin.srcDir("build/generated/ksp/main/kotlin/")
}

// Excludes for `kotlint`:
tasks.withType(org.jmailen.gradle.kotlinter.tasks.LintTask).all {
    exclude {
        it.name.endsWith(".Generated.kt")
    }
    exclude {
        it.name.endsWith("\$Extensions.kt")
    }
}
```

### Kotlin Multiplatform (JVM target only)
<details open>
<summary>Kotlin DSL</summary>

```kotlin
kotlin {
    jvm()
    sourceSets {
        val jvmMain by getting {
            kotlin.srcDir("build/generated/ksp/jvmMain/kotlin/")
        }
    }
}
```

</details>

<details open>
<summary>Groovy DSL</summary>

```groovy
kotlin {
    jvm()
    sourceSets {
        jvmMain {
            kotlin.srcDir("build/generated/ksp/jvmMain/kotlin/")
        }
    }
}
```

</details>

## Usage

### Annotation processing
Declare data schemas in your code and use them to access data in DataFrames.
A data schema is an interface with properties and no type parameters annotated with `@DataSchema`:
```kotlin
package org.example

import org.jetbrains.kotlinx.dataframe.annotations.DataSchema

@DataSchema
interface Person {
    val age: Int
}
```

### **Execute `build` task to generate type-safe accessors for schemas:** 

```kotlin
package org.example

import org.jetbrains.kotlinx.dataframe.annotations.DataSchema
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.filter
import org.jetbrains.kotlinx.dataframe.api.print
import org.jetbrains.kotlinx.dataframe.dataFrameOf

@DataSchema
interface Person {
    val age: Int
}

fun main() {
    val df = dataFrameOf("age")(5, 10, 15, 21).cast<Person>()
    // age only available after executing `build` or `kspKotlin`!
    val teens = df.filter { age in 10..19 }
    teens.print()
}
```

#### Visibility
For schemas with `internal` or `public` modifiers preprocessor will generate `internal` or `public` extensions

```kotlin
@DataSchema
internal interface Person {
    val age: Int
}
```

### Schema inference
Specify schema's configurations in `dataframes`  and execute the `build` task.
For the following configuration, file `Repository.Generated.kt` will be generated.
See [reference](#reference) and [examples](#examples) for more details.

#### build.gradle
```kotlin
dataframes {
    schema {
        data = "https://raw.githubusercontent.com/Kotlin/dataframe/master/data/jetbrains_repositories.csv"
        name = "org.example.Repository"
    }
}
```

```kotlin
package org.example

import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.api.cast
import org.jetbrains.kotlinx.dataframe.api.count
import org.jetbrains.kotlinx.dataframe.api.maxBy
import org.jetbrains.kotlinx.dataframe.api.print
import org.jetbrains.kotlinx.dataframe.io.read

const val REPOSITORIES_DATA = "https://raw.githubusercontent.com/Kotlin/dataframe/master/data/jetbrains_repositories.csv"

fun main() {
    val df = DataFrame.read(REPOSITORIES_DATA).cast<Repository>()
    // Use generated properties to access data in rows
    df.maxBy { stargazers_count }.print()
    // Or to access columns in dataframe.
    print(df.full_name.count { it.contains("kotlin") })
}
```

## Reference
```kotlin
dataframes {
    sourceSet = "mySources" // [optional; default: "main"]
    packageName = "org.jetbrains.data" // [optional; default: common package under source set]
    
    visibility = // [optional; default: if explicitApiMode enabled then EXPLICIT_PUBLIC, else IMPLICIT_PUBLIC]
    // KOTLIN SCRIPT: DataSchemaVisibility.INTERNAL DataSchemaVisibility.IMPLICIT_PUBLIC, DataSchemaVisibility.EXPLICIT_PUBLIC
    // GROOVY SCRIPT: 'internal', 'implicit_public', 'explicit_public'
    
    schema {
        sourceSet /* String */ = "…" // [optional; override default]
        packageName /* String */ = "…" // [optional; override default]
        visibility /* DataSchemaVisibility */ = "…" // [optional; override default]
        src /* File */ = file("…") // [optional; default: file("src/$sourceSet/kotlin")]
        
        data /* URL | File | String */ = "…" // Data in JSON or CSV formats
        name = "org.jetbrains.data.Person" // [optional; default: from filename]
    }
}
```

## Examples
In the best scenario, your schema could be defined as simple as this:
```kotlin
dataframes {
    // output: src/main/kotlin/org/example/dataframe/GeneratedSecurities.kt
    schema {
        data = "https://raw.githubusercontent.com/Kotlin/dataframe/1765966904c5920154a4a480aa1fcff23324f477/data/securities.csv"
    }
}
```
In this case output path will depend on your directory structure. For project with package `org.example` path will be `src/main/kotlin/org/example/dataframe/GeneratedSecurities.kt
`. Note that name of the Kotlin file is derived from the name of the data file with the prefix `Generated` and the package is derived from the directory structure with child directory `dataframe`. The name of the **data schema** itself is `Securities`. You could specify it explicitly:
```kotlin
schema {
    // output: src/main/kotlin/org/example/dataframe/GeneratedMyName.kt
    data = "https://raw.githubusercontent.com/Kotlin/dataframe/1765966904c5920154a4a480aa1fcff23324f477/data/securities.csv"
    name = "MyName"
}
```
If you want to change default package:
```kotlin
dataframes {
    packageName = "org.example"
    // Schemas...
}
```
Then you can set packageName for specific schema exclusively:
```kotlin
dataframes {
    // output: src/main/kotlin/org/example/data/GeneratedOtherName.kt
    schema {
        packageName = "org.example.data"
        data = file("path/to/data.csv")
    }
}
```
If you want non-default name and package, consider using fully-qualified name:
```kotlin
dataframes {
    // output: src/main/kotlin/org/example/data/GeneratedOtherName.kt
    schema {
        name = org.example.data.OtherName
        data = file("path/to/data.csv")
    }
}
```
By default, plugin will generate output in specified source set. Source set could be specified for all schemas or for specific schema:
```kotlin
dataframes {
    packageName = "org.example"
    sourceSet = "test"
    // output: src/test/kotlin/org/example/Data.kt
    schema {
        data = file("path/to/data.csv")
    }
    // output: src/integrationTest/kotlin/org/example/Data.kt
    schema {
        sourceSet = "integrationTest"
        data = file("path/to/data.csv")
    }
}
```
But if you need generated files in other directory, set `src`:
```kotlin
dataframes {
    // output: schemas/org/example/test/GeneratedOtherName.kt
    schema {
        data = "https://raw.githubusercontent.com/Kotlin/dataframe/1765966904c5920154a4a480aa1fcff23324f477/data/securities.csv"
        name = "org.example.test.OtherName"
        src = file("schemas")
    }
}
```
