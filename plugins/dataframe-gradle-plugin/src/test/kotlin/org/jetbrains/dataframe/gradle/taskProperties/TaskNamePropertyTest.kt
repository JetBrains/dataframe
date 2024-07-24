package org.jetbrains.dataframe.gradle.taskProperties

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.string.shouldContain
import org.gradle.api.ProjectConfigurationException
import org.jetbrains.dataframe.gradle.SchemaGeneratorExtension
import org.jetbrains.dataframe.gradle.SchemaGeneratorPlugin
import org.jetbrains.dataframe.gradle.makeProject
import org.junit.Test

class TaskNamePropertyTest {
    @Test
    fun `task name is last part of FQ name`() {
        val project = makeProject()
        project.plugins.apply(SchemaGeneratorPlugin::class.java)
        project.extensions.getByType(SchemaGeneratorExtension::class.java).apply {
            schema {
                data = "123"
                name = "org.example.my.321"
                src = project.projectDir
            }
        }
        project.evaluate()
        (project.tasks.findByName("generateDataFrame321") shouldNotBe null)
    }

    @Test
    fun `name from task property have higher priority then inferred from data`() {
        val project = makeProject()
        project.plugins.apply(SchemaGeneratorPlugin::class.java)
        project.extensions.getByType(SchemaGeneratorExtension::class.java).apply {
            schema {
                data = "/test/data.json"
                name = "org.example.my.321"
                src = project.projectDir
            }
        }
        project.evaluate()
        (project.tasks.findByName("generateDataFrame321") shouldNotBe null)
    }

    @Test
    fun `task name contains invalid characters`() {
        val project = makeProject()
        project.plugins.apply(SchemaGeneratorPlugin::class.java)
        project.extensions.getByType(SchemaGeneratorExtension::class.java).apply {
            schema {
                data = "123"
                name = "org.test.example.[321]"
                src = project.projectDir
            }
        }
        val exception = shouldThrow<ProjectConfigurationException> {
            project.evaluate()
        }
        exception.causes.single().message shouldContain "[321] contains illegal characters: [,]"
    }

    @Test
    fun `data name should not override invalid name`() {
        val project = makeProject()
        project.plugins.apply(SchemaGeneratorPlugin::class.java)
        project.extensions.getByType(SchemaGeneratorExtension::class.java).apply {
            schema {
                data =
                    "https://datalore-samples.s3-eu-west-1.amazonaws.com/datalore_gallery_of_samples/city_population.csv"
                name = "org.test.example.[321]"
                src = project.projectDir
            }
        }
        val exception = shouldThrow<ProjectConfigurationException> {
            project.evaluate()
        }
        exception.causes.single().message shouldContain "[321] contains illegal characters: [,]"
    }

    @Test
    fun `name convention is data file name`() {
        val project = makeProject()
        project.plugins.apply(SchemaGeneratorPlugin::class.java)
        project.extensions.getByType(SchemaGeneratorExtension::class.java).apply {
            schema {
                data =
                    "https://datalore-samples.s3-eu-west-1.amazonaws.com/datalore_gallery_of_samples/city_population.csv"
                packageName = ""
                src = project.projectDir
            }
        }
        project.evaluate()
        project.tasks.getByName("generateDataFrameCityPopulation") shouldNotBe null
    }
}
