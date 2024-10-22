package org.jetbrains.kotlinx.dataframe.api

import io.kotest.matchers.shouldBe
import org.jetbrains.kotlinx.dataframe.samples.api.age
import org.jetbrains.kotlinx.dataframe.samples.api.firstName
import org.jetbrains.kotlinx.dataframe.samples.api.lastName
import org.jetbrains.kotlinx.dataframe.samples.api.name
import org.jetbrains.kotlinx.dataframe.samples.api.secondName
import org.junit.Test

class SimplifyTests : ColumnsSelectionDslTests() {

    @Test
    fun simplify() {
        df.select {
            cols(name.firstName, name.lastName, age).simplify()
        } shouldBe df.select {
            cols(name.firstName, name.lastName, age)
        }

        df.select {
            cols(name.firstName, name.lastName, name.lastName, age, age, name).simplify()
        } shouldBe df.select {
            cols(name, age)
        }

        df.select {
            colsAtAnyDepth().simplify()
        } shouldBe df.select {
            all()
        }

        df.select {
            cols(name.firstName).simplify()
        } shouldBe df.select {
            cols(name.firstName)
        }

        dfGroup.select {
            (name and name.firstName.secondName and name.lastName).simplify()
        } shouldBe dfGroup.select {
            name
        }

        dfGroup.select {
            (name.firstName.secondName and name.lastName).simplify()
        } shouldBe dfGroup.select {
            name.firstName.secondName and name.lastName
        }
    }
}
