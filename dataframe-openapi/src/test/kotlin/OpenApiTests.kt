import io.kotest.assertions.throwables.shouldThrowAny
import io.kotest.matchers.booleans.shouldBeTrue
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.string.haveSubstring
import org.intellij.lang.annotations.Language
import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.api.isNotEmpty
import org.jetbrains.kotlinx.dataframe.api.schema
import org.jetbrains.kotlinx.dataframe.codeGen.ValidFieldName
import org.jetbrains.kotlinx.dataframe.io.OpenApi
import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.testkit.JupyterReplTestCase
import org.junit.Test
import java.io.File
import java.io.File.separatorChar
import java.io.InputStream

class OpenApiTests : JupyterReplTestCase() {

    private val openApi = OpenApi()
    private val additionalImports = openApi.createDefaultReadMethod().additionalImports.joinToString("\n")

    private fun execGeneratedCode(code: Code): Code {
        @Language("kts") val res1 = execRendered(
            """
            $additionalImports
            $code
            """.trimLines()
        )
        return code
    }

    private fun execGeneratedCode(file: File, name: String) =
        execGeneratedCode(
            code = openApi.readCodeForGeneration(
                file = file,
                name = name,
                extensionProperties = true,
                generateHelperCompanionObject = false,
            ),
        )

    private fun execGeneratedCode(stream: InputStream, name: String) =
        execGeneratedCode(
            code = openApi.readCodeForGeneration(
                stream = stream,
                name = name,
                extensionProperties = true,
                generateHelperCompanionObject = false,
            ),
        )

    private fun execGeneratedCode(text: String, name: String) =
        execGeneratedCode(
            code = openApi.readCodeForGeneration(
                text = text,
                name = name,
                extensionProperties = true,
                generateHelperCompanionObject = false,
            ),
        )

    private val petstoreJson = File("src/test/resources/petstore.json")
    private val petstoreAdvancedJson = File("src/test/resources/petstore_advanced.json")
    private val petstoreYaml = File("src/test/resources/petstore.yaml")
    private val someAdvancedPetsData = File("src/test/resources/some_advanced_pets.json").readText()
    private val someAdvancedOrdersData = File("src/test/resources/some_advanced_orders.json").readText()
    private val someAdvancedFailingOrdersData = File("src/test/resources/some_advanced_failing_orders.json").readText()
    private val advancedExample = File("src/test/resources/openapi_advanced_example.yaml")
    private val advancedData = File("src/test/resources/openapi_advanced_data.json").readText()
    private val advancedErrorData = File("src/test/resources/openapi_advanced_data2.json").readText()
    private val advancedErrorHolderData = File("src/test/resources/openapi_advanced_data3.json").readText()
    private val apiGuruYaml = File("src/test/resources/ApiGuruOpenApi.yaml")
    private val apiGuruData = File("src/test/resources/ApiGuruSample.json").readText()
    private val mlcYaml = File("src/test/resources/MlcGroupDataOpenApi.yaml")
    private val mlcLocationsWithPeopleData = File("src/test/resources/mlc_locations_with_people_data.json").readText()
    private val mlcPeopleWithLocationData = File("src/test/resources/mlc_people_with_location_data.json").readText()

    @Language("json")
    private val somePets = """
        [
            {
              "id": 0,
              "name": "doggie",
              "tag": "Dogs"
            },
            {
              "id": 1,
              "name": "kitty",
              "tag": "Cats"
            },
            {
              "id": 2,
              "name": "puppy"
            }
        ]
    """.trimLines()

    private val somePetsTripleQuotes = "\"\"\"$somePets\"\"\""

    private fun simpleTest(file: File) {
        val fullFunctionName = ValidFieldName.of(::simpleTest.name)
        val functionName = fullFunctionName.quotedIfNeeded
        val code = execGeneratedCode(file, fullFunctionName.unquoted).trimLines()

        @Language("kt") val petInterface = """
            @DataSchema(isOpen = false)
            interface Pet {
                val id: kotlin.Long
                val name: kotlin.String
                val tag: kotlin.String?
                public companion object {
        """.trimLines()

        code should haveSubstring(petInterface)

        @Language("kt") val petExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long> @JvmName("Pet_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.id: kotlin.Long @JvmName("Pet_id") get() = this["id"] as kotlin.Long
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullablePet_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.id: kotlin.Long? @JvmName("NullablePet_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Pet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.name: kotlin.String @JvmName("Pet_name") get() = this["name"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.name: kotlin.String? @JvmName("NullablePet_name") get() = this["name"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Pet_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.tag: kotlin.String? @JvmName("Pet_tag") get() = this["tag"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.tag: kotlin.String? @JvmName("NullablePet_tag") get() = this["tag"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(petExtensions)

        @Language("kt") val petsTypeAlias = """
            typealias Pets = org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Pet>
        """.trimLines()

        code should haveSubstring(petsTypeAlias)

        @Language("kt") val errorInterface = """
            @DataSchema(isOpen = false)
            interface Error {
                val code: kotlin.Int
                val message: kotlin.String
                public companion object {
        """.trimLines()

        code should haveSubstring(errorInterface)

        @Language("kt") val errorExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Error>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int> @JvmName("Error_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Error>.code: kotlin.Int @JvmName("Error_code") get() = this["code"] as kotlin.Int
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Error?>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableError_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Error?>.code: kotlin.Int? @JvmName("NullableError_code") get() = this["code"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Error>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Error_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Error>.message: kotlin.String @JvmName("Error_message") get() = this["message"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Error?>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableError_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Error?>.message: kotlin.String? @JvmName("NullableError_message") get() = this["message"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(errorExtensions)

        @Language("kts") val res2 = execRaw("$functionName.Pet.readJsonStr($somePetsTripleQuotes)") as AnyFrame
    }

    @Test
    fun `Simple test Petstore Json`() {
        simpleTest(petstoreJson)
    }

    @Test
    fun `Simple test Petstore Yaml`() {
        simpleTest(petstoreYaml)
    }

    @Test
    fun `Advanced test Petstore Json`() {
        val fullFunctionName = ValidFieldName.of(::`Advanced test Petstore Json`.name)
        val functionName = fullFunctionName.quotedIfNeeded

        val code = execGeneratedCode(
            file = petstoreAdvancedJson,
            name = fullFunctionName.unquoted,
        ).trimLines()

        @Language("kts") val statusInterface = """
            enum class Status(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                PLACED("placed"),
                APPROVED("approved"),
                DELIVERED("delivered");
            }
        """.trimLines()

        code should haveSubstring(statusInterface)

        @Language("kt") val orderInterface = """
            @DataSchema(isOpen = false)
            interface Order {
                val id: kotlin.Long?
                val petId: kotlin.Long?
                val quantity: kotlin.Int?
                val shipDate: kotlinx.datetime.LocalDateTime?
                val status: $functionName.Status?
                val complete: kotlin.Boolean?
                public companion object {
        """.trimLines()

        code should haveSubstring(orderInterface)

        @Language("kt") val customerInterface = """
            @DataSchema(isOpen = false)
            interface Customer {
                val id: kotlin.Long?
                val username: kotlin.String?
                val address: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>
                public companion object {
        """.trimLines() // address is a nullable array of objects -> DataFrame<Address?>

        code should haveSubstring(customerInterface)

        @Language("kt") val customerExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer>.address: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>> @JvmName("Customer_address") get() = this["address"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer>.address: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?> @JvmName("Customer_address") get() = this["address"] as org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer?>.address: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>> @JvmName("NullableCustomer_address") get() = this["address"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer?>.address: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?> @JvmName("NullableCustomer_address") get() = this["address"] as org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Address?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("Customer_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer>.id: kotlin.Long? @JvmName("Customer_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullableCustomer_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer?>.id: kotlin.Long? @JvmName("NullableCustomer_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer>.username: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Customer_username") get() = this["username"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer>.username: kotlin.String? @JvmName("Customer_username") get() = this["username"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Customer?>.username: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableCustomer_username") get() = this["username"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Customer?>.username: kotlin.String? @JvmName("NullableCustomer_username") get() = this["username"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(customerExtensions)

        @Language("kt") val orderExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.complete: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("Order_complete") get() = this["complete"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.complete: kotlin.Boolean? @JvmName("Order_complete") get() = this["complete"] as kotlin.Boolean?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.complete: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("NullableOrder_complete") get() = this["complete"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.complete: kotlin.Boolean? @JvmName("NullableOrder_complete") get() = this["complete"] as kotlin.Boolean?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("Order_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.id: kotlin.Long? @JvmName("Order_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullableOrder_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.id: kotlin.Long? @JvmName("NullableOrder_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.petId: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("Order_petId") get() = this["petId"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.petId: kotlin.Long? @JvmName("Order_petId") get() = this["petId"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.petId: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullableOrder_petId") get() = this["petId"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.petId: kotlin.Long? @JvmName("NullableOrder_petId") get() = this["petId"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.quantity: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("Order_quantity") get() = this["quantity"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.quantity: kotlin.Int? @JvmName("Order_quantity") get() = this["quantity"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.quantity: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableOrder_quantity") get() = this["quantity"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.quantity: kotlin.Int? @JvmName("NullableOrder_quantity") get() = this["quantity"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.shipDate: org.jetbrains.kotlinx.dataframe.DataColumn<kotlinx.datetime.LocalDateTime?> @JvmName("Order_shipDate") get() = this["shipDate"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlinx.datetime.LocalDateTime?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.shipDate: kotlinx.datetime.LocalDateTime? @JvmName("Order_shipDate") get() = this["shipDate"] as kotlinx.datetime.LocalDateTime?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.shipDate: org.jetbrains.kotlinx.dataframe.DataColumn<kotlinx.datetime.LocalDateTime?> @JvmName("NullableOrder_shipDate") get() = this["shipDate"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlinx.datetime.LocalDateTime?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.shipDate: kotlinx.datetime.LocalDateTime? @JvmName("NullableOrder_shipDate") get() = this["shipDate"] as kotlinx.datetime.LocalDateTime?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order>.status: org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status?> @JvmName("Order_status") get() = this["status"] as org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order>.status: $functionName.Status? @JvmName("Order_status") get() = this["status"] as $functionName.Status?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Order?>.status: org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status?> @JvmName("NullableOrder_status") get() = this["status"] as org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Order?>.status: $functionName.Status? @JvmName("NullableOrder_status") get() = this["status"] as $functionName.Status?
        """.trimLines()

        code should haveSubstring(orderExtensions)

        @Language("kt") val addressInterface = """
            @DataSchema(isOpen = false)
            interface Address {
                val street: kotlin.String?
                val city: kotlin.String?
                val state: kotlin.String?
                val zip: kotlin.String?
                public companion object {
        """.trimLines()

        code should haveSubstring(addressInterface)

        @Language("kt") val addressExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address>.city: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Address_city") get() = this["city"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address>.city: kotlin.String? @JvmName("Address_city") get() = this["city"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address?>.city: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableAddress_city") get() = this["city"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address?>.city: kotlin.String? @JvmName("NullableAddress_city") get() = this["city"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address>.state: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Address_state") get() = this["state"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address>.state: kotlin.String? @JvmName("Address_state") get() = this["state"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address?>.state: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableAddress_state") get() = this["state"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address?>.state: kotlin.String? @JvmName("NullableAddress_state") get() = this["state"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address>.street: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Address_street") get() = this["street"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address>.street: kotlin.String? @JvmName("Address_street") get() = this["street"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address?>.street: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableAddress_street") get() = this["street"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address?>.street: kotlin.String? @JvmName("NullableAddress_street") get() = this["street"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address>.zip: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Address_zip") get() = this["zip"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address>.zip: kotlin.String? @JvmName("Address_zip") get() = this["zip"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Address?>.zip: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableAddress_zip") get() = this["zip"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Address?>.zip: kotlin.String? @JvmName("NullableAddress_zip") get() = this["zip"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(addressExtensions)

        @Language("kt") val categoryInterface = """
            @DataSchema(isOpen = false)
            interface Category {
                val id: kotlin.Long?
                val name: kotlin.String?
                public companion object {
        """.trimLines()

        code should haveSubstring(categoryInterface)

        @Language("kt") val categoryExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Category>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("Category_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category>.id: kotlin.Long? @JvmName("Category_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Category?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullableCategory_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?>.id: kotlin.Long? @JvmName("NullableCategory_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Category>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Category_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category>.name: kotlin.String? @JvmName("Category_name") get() = this["name"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Category?>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableCategory_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?>.name: kotlin.String? @JvmName("NullableCategory_name") get() = this["name"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(categoryExtensions)

        @Language("kt") val userInterface = """
            @DataSchema(isOpen = false)
            interface User {
                val id: kotlin.Long?
                val username: kotlin.String?
                val firstName: kotlin.String?
                val lastName: kotlin.String?
                val email: kotlin.String?
                val password: kotlin.String?
                val phone: kotlin.String?
                val userStatus: kotlin.Int?
                public companion object {
        """.trimLines()

        code should haveSubstring(userInterface)

        @Language("kt") val userExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.email: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_email") get() = this["email"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.email: kotlin.String? @JvmName("User_email") get() = this["email"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.email: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_email") get() = this["email"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.email: kotlin.String? @JvmName("NullableUser_email") get() = this["email"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.firstName: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_firstName") get() = this["firstName"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.firstName: kotlin.String? @JvmName("User_firstName") get() = this["firstName"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.firstName: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_firstName") get() = this["firstName"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.firstName: kotlin.String? @JvmName("NullableUser_firstName") get() = this["firstName"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("User_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.id: kotlin.Long? @JvmName("User_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullableUser_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.id: kotlin.Long? @JvmName("NullableUser_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.lastName: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_lastName") get() = this["lastName"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.lastName: kotlin.String? @JvmName("User_lastName") get() = this["lastName"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.lastName: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_lastName") get() = this["lastName"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.lastName: kotlin.String? @JvmName("NullableUser_lastName") get() = this["lastName"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.password: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_password") get() = this["password"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.password: kotlin.String? @JvmName("User_password") get() = this["password"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.password: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_password") get() = this["password"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.password: kotlin.String? @JvmName("NullableUser_password") get() = this["password"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.phone: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_phone") get() = this["phone"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.phone: kotlin.String? @JvmName("User_phone") get() = this["phone"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.phone: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_phone") get() = this["phone"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.phone: kotlin.String? @JvmName("NullableUser_phone") get() = this["phone"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.userStatus: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("User_userStatus") get() = this["userStatus"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.userStatus: kotlin.Int? @JvmName("User_userStatus") get() = this["userStatus"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.userStatus: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableUser_userStatus") get() = this["userStatus"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.userStatus: kotlin.Int? @JvmName("NullableUser_userStatus") get() = this["userStatus"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User>.username: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("User_username") get() = this["username"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User>.username: kotlin.String? @JvmName("User_username") get() = this["username"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.User?>.username: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableUser_username") get() = this["username"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.User?>.username: kotlin.String? @JvmName("NullableUser_username") get() = this["username"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(userExtensions)

        @Language("kt") val tagInterface = """
            @DataSchema(isOpen = false)
            interface Tag {
                val id: kotlin.Long?
                val name: kotlin.String?
                public companion object {
        """.trimLines()

        code should haveSubstring(tagInterface)

        @Language("kt") val status1Enum = """
            enum class Status1(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                AVAILABLE("available"),
                PENDING("pending"),
                SOLD("sold");
            }
        """.trimLines()

        code should haveSubstring(status1Enum)

        @Language("kt") val petInterface = """
            @DataSchema(isOpen = false)
            interface Pet {
                val id: kotlin.Long?
                val name: kotlin.String
                val category: $functionName.Category?
                val photoUrls: kotlin.collections.List<kotlin.String>
                val tags: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>
                val status: $functionName.Status1?
                public companion object {
        """.trimLines() // category is a single other object, photoUrls is a primitive array, tags is a nullable array of objects

        code should haveSubstring(petInterface)

        @Language("kt") val petExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.category: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<$functionName.Category?> @JvmName("Pet_category") get() = this["category"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<$functionName.Category?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.category: org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?> @JvmName("Pet_category") get() = this["category"] as org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.category: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<$functionName.Category?> @JvmName("NullablePet_category") get() = this["category"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<$functionName.Category?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.category: org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?> @JvmName("NullablePet_category") get() = this["category"] as org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Category?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("Pet_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.id: kotlin.Long? @JvmName("Pet_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.id: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?> @JvmName("NullablePet_id") get() = this["id"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Long?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.id: kotlin.Long? @JvmName("NullablePet_id") get() = this["id"] as kotlin.Long?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Pet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.name: kotlin.String @JvmName("Pet_name") get() = this["name"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.name: kotlin.String? @JvmName("NullablePet_name") get() = this["name"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.photoUrls: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.String>> @JvmName("Pet_photoUrls") get() = this["photoUrls"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.String>>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.photoUrls: kotlin.collections.List<kotlin.String> @JvmName("Pet_photoUrls") get() = this["photoUrls"] as kotlin.collections.List<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.photoUrls: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.String>?> @JvmName("NullablePet_photoUrls") get() = this["photoUrls"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.String>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.photoUrls: kotlin.collections.List<kotlin.String>? @JvmName("NullablePet_photoUrls") get() = this["photoUrls"] as kotlin.collections.List<kotlin.String>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.status: org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status1?> @JvmName("Pet_status") get() = this["status"] as org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status1?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.status: $functionName.Status1? @JvmName("Pet_status") get() = this["status"] as $functionName.Status1?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.status: org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status1?> @JvmName("NullablePet_status") get() = this["status"] as org.jetbrains.kotlinx.dataframe.DataColumn<$functionName.Status1?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.status: $functionName.Status1? @JvmName("NullablePet_status") get() = this["status"] as $functionName.Status1?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet>.tags: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>> @JvmName("Pet_tags") get() = this["tags"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet>.tags: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?> @JvmName("Pet_tags") get() = this["tags"] as org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.Pet?>.tags: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>> @JvmName("NullablePet_tags") get() = this["tags"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.Pet?>.tags: org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?> @JvmName("NullablePet_tags") get() = this["tags"] as org.jetbrains.kotlinx.dataframe.DataFrame<$functionName.Tag?>
        """.trimLines()

        code should haveSubstring(petExtensions)

        @Language("kt") val apiResponseInterface = """
            @DataSchema(isOpen = false)
            interface ApiResponse {
                val code: kotlin.Int?
                val type: kotlin.String?
                val message: kotlin.String?
                public companion object {
        """.trimLines()

        code should haveSubstring(apiResponseInterface)

        @Language("kt") val apiResponseExtensions = """
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("ApiResponse_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse>.code: kotlin.Int? @JvmName("ApiResponse_code") get() = this["code"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse?>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableApiResponse_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse?>.code: kotlin.Int? @JvmName("NullableApiResponse_code") get() = this["code"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("ApiResponse_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse>.message: kotlin.String? @JvmName("ApiResponse_message") get() = this["message"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse?>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableApiResponse_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse?>.message: kotlin.String? @JvmName("NullableApiResponse_message") get() = this["message"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse>.type: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("ApiResponse_type") get() = this["type"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse>.type: kotlin.String? @JvmName("ApiResponse_type") get() = this["type"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<$functionName.ApiResponse?>.type: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableApiResponse_type") get() = this["type"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<$functionName.ApiResponse?>.type: kotlin.String? @JvmName("NullableApiResponse_type") get() = this["type"] as kotlin.String?
        """.trimLines()

        code should haveSubstring(apiResponseExtensions)

        @Language("kts") val res2 =
            execRaw("$functionName.Pet.readJsonStr(\"\"\"$someAdvancedPetsData\"\"\")") as AnyFrame

        @Language("kts") val res3 =
            execRaw("$functionName.Order.readJsonStr(\"\"\"$someAdvancedOrdersData\"\"\")") as AnyFrame

        shouldThrowAny {
            @Language("kts") val res4 =
                execRaw("$functionName.Order.readJsonStr(\"\"\"$someAdvancedFailingOrdersData\"\"\")") as AnyFrame
            res4
        }
    }

    @Test
    fun `Check advances code generated`() {
        val fullFunctionName = ValidFieldName.of(::`Other advanced test`.name)
        val code = openApi.readCodeForGeneration(
            file = advancedExample,
            name = fullFunctionName.unquoted,
            extensionProperties = true,
            generateHelperCompanionObject = false,
        )
            .trimLines()

        @Language("kt") val s = """
            interface `Other advanced test` {
                
                enum class EyeColor(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                    BLUE("Blue"),
                    YELLOW("Yellow"),
                    BROWN("Brown"),
                    GREEN("Green");
                }
                @DataSchema(isOpen = false)
                interface Pet {
                    @ColumnName("pet_type")
                    val petType: kotlin.String
                    @ColumnName("value")
                    val `value`: kotlin.Any?
                    val name: kotlin.String
                    val tag: kotlin.String?
                    val other: kotlin.Any?
                    @ColumnName("eye_color")
                    val eyeColor: `Other advanced test`.EyeColor?
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf()
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToPet(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<Pet>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<Pet> = convertTo<Pet> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<Pet> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToPet()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Pet> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToPet()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<Pet> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToPet()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Pet> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToPet()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface Dog : `Other advanced test`.Pet {
                    override val tag: kotlin.String
                    val bark: kotlin.Boolean?
                    val breed: `Other advanced test`.Breed
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf()
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToDog(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<Dog>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<Dog> = convertTo<Dog> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<Dog> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToDog()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Dog> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToDog()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<Dog> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToDog()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Dog> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToDog()
                    }
                
                }
                enum class Breed(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                    DINGO("Dingo"),
                    HUSKY("Husky"),
                    RETRIEVER("Retriever"),
                    SHEPHERD("Shepherd");
                }
                @DataSchema(isOpen = false)
                interface Cat : `Other advanced test`.Pet {
                    val hunts: kotlin.Boolean?
                    val age: kotlin.Float?
                    val breed: `Other advanced test`.Breed1?
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf()
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToCat(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<Cat>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<Cat> = convertTo<Cat> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<Cat> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToCat()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Cat> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToCat()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<Cat> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToCat()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Cat> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToCat()
                    }
                
                }
                enum class Breed1(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                    RAGDOLL("Ragdoll"),
                    SHORTHAIR("Shorthair"),
                    PERSIAN("Persian"),
                    MAINE_COON("Maine Coon"),
                    MAINE_COON_1("maine_coon"),
                    EMPTY_STRING(""),
                    `1`("1");
                }
                @DataSchema(isOpen = false)
                interface IntList {
                    val list: kotlin.collections.List<kotlin.Int>
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf()
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToIntList(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<IntList>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<IntList> = convertTo<IntList> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<IntList> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToIntList()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<IntList> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToIntList()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<IntList> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToIntList()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<IntList> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToIntList()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface ObjectWithAdditionalProperties : org.jetbrains.kotlinx.dataframe.io.AdditionalProperty<kotlin.String> {
                    @ColumnName("value")
                    override val `value`: kotlin.String
                    override val key: kotlin.String
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToObjectWithAdditionalProperties(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<ObjectWithAdditionalProperties>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditionalProperties> = convertTo<ObjectWithAdditionalProperties> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditionalProperties> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditionalProperties()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditionalProperties> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditionalProperties()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditionalProperties> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditionalProperties()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditionalProperties> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditionalProperties()
                    }
                
                }
                enum class Op(override val value: kotlin.String) : org.jetbrains.kotlinx.dataframe.api.DataSchemaEnum {
                    ADD("add"),
                    REMOVE("remove"),
                    REPLACE("replace");
                }
                @DataSchema(isOpen = false)
                interface Value {
                
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf()
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToValue(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<Value>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<Value> = convertTo<Value> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<Value> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToValue()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Value> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToValue()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<Value> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToValue()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Value> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToValue()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface ObjectWithAdditional : org.jetbrains.kotlinx.dataframe.io.AdditionalProperty<kotlin.Int> {
                    @ColumnName("value")
                    override val `value`: kotlin.Int
                    override val key: kotlin.String
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToObjectWithAdditional(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<ObjectWithAdditional>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional> = convertTo<ObjectWithAdditional> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface SomeArrayContent {
                    val op: `Other advanced test`.Op
                    val path: kotlin.String
                    @ColumnName("value")
                    val `value`: `Other advanced test`.Value?
                    val objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}["objectWithAdditional"]""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToSomeArrayContent(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<SomeArrayContent>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<SomeArrayContent> = convertTo<SomeArrayContent> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<SomeArrayContent> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToSomeArrayContent()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<SomeArrayContent> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToSomeArrayContent()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<SomeArrayContent> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToSomeArrayContent()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<SomeArrayContent> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToSomeArrayContent()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface Error {
                    val ints: `Other advanced test`.IntList?
                    val petRef: PetRef
                    val pets: org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>
                    val code: kotlin.Int
                    val message: kotlin.String
                    val objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>
                    val objectWithAdditionalList: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?
                    val objectWithAdditional2: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>
                    val objectWithAdditional3: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3>
                    val array: kotlin.collections.List<SomeArrayArray>?
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}["objectWithAdditional"]""${'"'}), JsonPath(""${'"'}${'$'}["objectWithAdditionalList"][*]""${'"'}), JsonPath(""${'"'}${'$'}["objectWithAdditional2"]""${'"'}), JsonPath(""${'"'}${'$'}["objectWithAdditional3"]""${'"'}), JsonPath(""${'"'}${'$'}["array"][*][*][*]["objectWithAdditional"]""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToError(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<Error>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<Error> = convertTo<Error> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<Error> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToError()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Error> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToError()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<Error> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToError()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<Error> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToError()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface ObjectWithAdditional2 : org.jetbrains.kotlinx.dataframe.io.AdditionalProperty<kotlin.Any> {
                    @ColumnName("value")
                    override val `value`: kotlin.Any
                    override val key: kotlin.String
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToObjectWithAdditional2(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<ObjectWithAdditional2>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional2> = convertTo<ObjectWithAdditional2> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional2> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional2()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional2> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional2()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional2> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional2()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional2> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional2()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface ObjectWithAdditional3 : org.jetbrains.kotlinx.dataframe.io.AdditionalProperty<kotlin.Any?> {
                    @ColumnName("value")
                    override val `value`: kotlin.Any?
                    override val key: kotlin.String
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToObjectWithAdditional3(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<ObjectWithAdditional3>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional3> = convertTo<ObjectWithAdditional3> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional3> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional3()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional3> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional3()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional3> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional3()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ObjectWithAdditional3> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)["value"].first().let { it as DataFrame<*> }
                          .convertToObjectWithAdditional3()
                    }
                
                }
                @DataSchema(isOpen = false)
                interface ErrorHolder {
                    val errors: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error>
                    public companion object {
                      public val keyValuePaths: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.api.JsonPath>
                        get() = listOf(JsonPath(""${'"'}${'$'}["errors"][*]["objectWithAdditional"]""${'"'}), JsonPath(""${'"'}${'$'}["errors"][*]["objectWithAdditionalList"][*]""${'"'}), JsonPath(""${'"'}${'$'}["errors"][*]["objectWithAdditional2"]""${'"'}), JsonPath(""${'"'}${'$'}["errors"][*]["objectWithAdditional3"]""${'"'}), JsonPath(""${'"'}${'$'}["errors"][*]["array"][*][*][*]["objectWithAdditional"]""${'"'}))
                
                      public fun org.jetbrains.kotlinx.dataframe.DataFrame<*>.convertToErrorHolder(convertTo: org.jetbrains.kotlinx.dataframe.api.ConvertSchemaDsl<ErrorHolder>.() -> kotlin.Unit = {}): org.jetbrains.kotlinx.dataframe.DataFrame<ErrorHolder> = convertTo<ErrorHolder> { 
                          convertDataRowsWithOpenApi() 
                          convertTo()
                      }
                
                      public fun readJson(url: java.net.URL): org.jetbrains.kotlinx.dataframe.DataFrame<ErrorHolder> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(url, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToErrorHolder()
                
                      public fun readJson(path: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ErrorHolder> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(path, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToErrorHolder()
                
                      public fun readJson(stream: java.io.InputStream): org.jetbrains.kotlinx.dataframe.DataFrame<ErrorHolder> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJson(stream, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToErrorHolder()
                
                      public fun readJsonStr(text: kotlin.String): org.jetbrains.kotlinx.dataframe.DataFrame<ErrorHolder> = org.jetbrains.kotlinx.dataframe.DataFrame
                          .readJsonStr(text, typeClashTactic = ANY_COLUMNS, keyValuePaths = keyValuePaths)
                          .convertToErrorHolder()
                    }
                
                }
            }
            typealias PetRef = `Other advanced test`.Pet
            typealias AlsoCat = `Other advanced test`.Cat
            typealias Integer = kotlin.Int
            typealias SomeArray = org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.SomeArrayContent>
            typealias SomeArrayArray = kotlin.collections.List<SomeArray>

            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("Pet_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.`value`: kotlin.Any? @JvmName("Pet_value") get() = this["value"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("NullablePet_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.`value`: kotlin.Any? @JvmName("NullablePet_value") get() = this["value"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.eyeColor: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.EyeColor?> @JvmName("Pet_eyeColor") get() = this["eye_color"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.EyeColor?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.eyeColor: `Other advanced test`.EyeColor? @JvmName("Pet_eyeColor") get() = this["eye_color"] as `Other advanced test`.EyeColor?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.eyeColor: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.EyeColor?> @JvmName("NullablePet_eyeColor") get() = this["eye_color"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.EyeColor?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.eyeColor: `Other advanced test`.EyeColor? @JvmName("NullablePet_eyeColor") get() = this["eye_color"] as `Other advanced test`.EyeColor?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Pet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.name: kotlin.String @JvmName("Pet_name") get() = this["name"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.name: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_name") get() = this["name"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.name: kotlin.String? @JvmName("NullablePet_name") get() = this["name"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.other: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("Pet_other") get() = this["other"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.other: kotlin.Any? @JvmName("Pet_other") get() = this["other"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.other: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("NullablePet_other") get() = this["other"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.other: kotlin.Any? @JvmName("NullablePet_other") get() = this["other"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.petType: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Pet_petType") get() = this["pet_type"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.petType: kotlin.String @JvmName("Pet_petType") get() = this["pet_type"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.petType: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_petType") get() = this["pet_type"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.petType: kotlin.String? @JvmName("NullablePet_petType") get() = this["pet_type"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("Pet_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet>.tag: kotlin.String? @JvmName("Pet_tag") get() = this["tag"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Pet?>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullablePet_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Pet?>.tag: kotlin.String? @JvmName("NullablePet_tag") get() = this["tag"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog>.bark: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("Dog_bark") get() = this["bark"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog>.bark: kotlin.Boolean? @JvmName("Dog_bark") get() = this["bark"] as kotlin.Boolean?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog?>.bark: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("NullableDog_bark") get() = this["bark"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog?>.bark: kotlin.Boolean? @JvmName("NullableDog_bark") get() = this["bark"] as kotlin.Boolean?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog>.breed: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed> @JvmName("Dog_breed") get() = this["breed"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog>.breed: `Other advanced test`.Breed @JvmName("Dog_breed") get() = this["breed"] as `Other advanced test`.Breed
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog?>.breed: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed?> @JvmName("NullableDog_breed") get() = this["breed"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog?>.breed: `Other advanced test`.Breed? @JvmName("NullableDog_breed") get() = this["breed"] as `Other advanced test`.Breed?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Dog_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog>.tag: kotlin.String @JvmName("Dog_tag") get() = this["tag"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Dog?>.tag: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableDog_tag") get() = this["tag"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Dog?>.tag: kotlin.String? @JvmName("NullableDog_tag") get() = this["tag"] as kotlin.String?

            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat>.age: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Float?> @JvmName("Cat_age") get() = this["age"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Float?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat>.age: kotlin.Float? @JvmName("Cat_age") get() = this["age"] as kotlin.Float?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat?>.age: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Float?> @JvmName("NullableCat_age") get() = this["age"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Float?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat?>.age: kotlin.Float? @JvmName("NullableCat_age") get() = this["age"] as kotlin.Float?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat>.breed: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed1?> @JvmName("Cat_breed") get() = this["breed"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed1?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat>.breed: `Other advanced test`.Breed1? @JvmName("Cat_breed") get() = this["breed"] as `Other advanced test`.Breed1?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat?>.breed: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed1?> @JvmName("NullableCat_breed") get() = this["breed"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Breed1?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat?>.breed: `Other advanced test`.Breed1? @JvmName("NullableCat_breed") get() = this["breed"] as `Other advanced test`.Breed1?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat>.hunts: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("Cat_hunts") get() = this["hunts"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat>.hunts: kotlin.Boolean? @JvmName("Cat_hunts") get() = this["hunts"] as kotlin.Boolean?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Cat?>.hunts: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?> @JvmName("NullableCat_hunts") get() = this["hunts"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Boolean?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Cat?>.hunts: kotlin.Boolean? @JvmName("NullableCat_hunts") get() = this["hunts"] as kotlin.Boolean?




            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.IntList>.list: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.Int>> @JvmName("IntList_list") get() = this["list"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.Int>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList>.list: kotlin.collections.List<kotlin.Int> @JvmName("IntList_list") get() = this["list"] as kotlin.collections.List<kotlin.Int>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.IntList?>.list: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.Int>?> @JvmName("NullableIntList_list") get() = this["list"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<kotlin.Int>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList?>.list: kotlin.collections.List<kotlin.Int>? @JvmName("NullableIntList_list") get() = this["list"] as kotlin.collections.List<kotlin.Int>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditionalProperties>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("ObjectWithAdditionalProperties_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditionalProperties>.`value`: kotlin.String @JvmName("ObjectWithAdditionalProperties_value") get() = this["value"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditionalProperties?>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableObjectWithAdditionalProperties_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditionalProperties?>.`value`: kotlin.String? @JvmName("NullableObjectWithAdditionalProperties_value") get() = this["value"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditionalProperties>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("ObjectWithAdditionalProperties_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditionalProperties>.key: kotlin.String @JvmName("ObjectWithAdditionalProperties_key") get() = this["key"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditionalProperties?>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableObjectWithAdditionalProperties_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditionalProperties?>.key: kotlin.String? @JvmName("NullableObjectWithAdditionalProperties_key") get() = this["key"] as kotlin.String?



            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int> @JvmName("ObjectWithAdditional_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional>.`value`: kotlin.Int @JvmName("ObjectWithAdditional_value") get() = this["value"] as kotlin.Int
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional?>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableObjectWithAdditional_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional?>.`value`: kotlin.Int? @JvmName("NullableObjectWithAdditional_value") get() = this["value"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("ObjectWithAdditional_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional>.key: kotlin.String @JvmName("ObjectWithAdditional_key") get() = this["key"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional?>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableObjectWithAdditional_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional?>.key: kotlin.String? @JvmName("NullableObjectWithAdditional_key") get() = this["key"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent>.`value`: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.Value?> @JvmName("SomeArrayContent_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.Value?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent>.`value`: org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Value?> @JvmName("SomeArrayContent_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Value?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent?>.`value`: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.Value?> @JvmName("NullableSomeArrayContent_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.Value?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent?>.`value`: org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Value?> @JvmName("NullableSomeArrayContent_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Value?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>> @JvmName("SomeArrayContent_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?> @JvmName("SomeArrayContent_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent?>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>> @JvmName("NullableSomeArrayContent_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent?>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?> @JvmName("NullableSomeArrayContent_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent>.op: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Op> @JvmName("SomeArrayContent_op") get() = this["op"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Op>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent>.op: `Other advanced test`.Op @JvmName("SomeArrayContent_op") get() = this["op"] as `Other advanced test`.Op
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent?>.op: org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Op?> @JvmName("NullableSomeArrayContent_op") get() = this["op"] as org.jetbrains.kotlinx.dataframe.DataColumn<`Other advanced test`.Op?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent?>.op: `Other advanced test`.Op? @JvmName("NullableSomeArrayContent_op") get() = this["op"] as `Other advanced test`.Op?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent>.path: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("SomeArrayContent_path") get() = this["path"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent>.path: kotlin.String @JvmName("SomeArrayContent_path") get() = this["path"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.SomeArrayContent?>.path: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableSomeArrayContent_path") get() = this["path"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.SomeArrayContent?>.path: kotlin.String? @JvmName("NullableSomeArrayContent_path") get() = this["path"] as kotlin.String?

            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.array: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<SomeArrayArray>?> @JvmName("Error_array") get() = this["array"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<SomeArrayArray>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.array: kotlin.collections.List<SomeArrayArray>? @JvmName("Error_array") get() = this["array"] as kotlin.collections.List<SomeArrayArray>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.array: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<SomeArrayArray>?> @JvmName("NullableError_array") get() = this["array"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<SomeArrayArray>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.array: kotlin.collections.List<SomeArrayArray>? @JvmName("NullableError_array") get() = this["array"] as kotlin.collections.List<SomeArrayArray>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int> @JvmName("Error_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.code: kotlin.Int @JvmName("Error_code") get() = this["code"] as kotlin.Int
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.code: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?> @JvmName("NullableError_code") get() = this["code"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Int?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.code: kotlin.Int? @JvmName("NullableError_code") get() = this["code"] as kotlin.Int?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.ints: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.IntList?> @JvmName("Error_ints") get() = this["ints"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.IntList?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.ints: org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList?> @JvmName("Error_ints") get() = this["ints"] as org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.ints: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.IntList?> @JvmName("NullableError_ints") get() = this["ints"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<`Other advanced test`.IntList?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.ints: org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList?> @JvmName("NullableError_ints") get() = this["ints"] as org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.IntList?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("Error_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.message: kotlin.String @JvmName("Error_message") get() = this["message"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.message: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableError_message") get() = this["message"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.message: kotlin.String? @JvmName("NullableError_message") get() = this["message"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>> @JvmName("Error_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties> @JvmName("Error_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties?>> @JvmName("NullableError_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.objectWithAdditional: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties?> @JvmName("NullableError_objectWithAdditional") get() = this["objectWithAdditional"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.objectWithAdditional2: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>> @JvmName("Error_objectWithAdditional2") get() = this["objectWithAdditional2"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.objectWithAdditional2: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?> @JvmName("Error_objectWithAdditional2") get() = this["objectWithAdditional2"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.objectWithAdditional2: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>> @JvmName("NullableError_objectWithAdditional2") get() = this["objectWithAdditional2"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.objectWithAdditional2: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?> @JvmName("NullableError_objectWithAdditional2") get() = this["objectWithAdditional2"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional2?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.objectWithAdditional3: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3>> @JvmName("Error_objectWithAdditional3") get() = this["objectWithAdditional3"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.objectWithAdditional3: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3> @JvmName("Error_objectWithAdditional3") get() = this["objectWithAdditional3"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.objectWithAdditional3: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3?>> @JvmName("NullableError_objectWithAdditional3") get() = this["objectWithAdditional3"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.objectWithAdditional3: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3?> @JvmName("NullableError_objectWithAdditional3") get() = this["objectWithAdditional3"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditional3?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.objectWithAdditionalList: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?> @JvmName("Error_objectWithAdditionalList") get() = this["objectWithAdditionalList"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.objectWithAdditionalList: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>? @JvmName("Error_objectWithAdditionalList") get() = this["objectWithAdditionalList"] as kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.objectWithAdditionalList: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?> @JvmName("NullableError_objectWithAdditionalList") get() = this["objectWithAdditionalList"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.objectWithAdditionalList: kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>? @JvmName("NullableError_objectWithAdditionalList") get() = this["objectWithAdditionalList"] as kotlin.collections.List<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.ObjectWithAdditionalProperties>>?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.petRef: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<PetRef> @JvmName("Error_petRef") get() = this["petRef"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<PetRef>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.petRef: org.jetbrains.kotlinx.dataframe.DataRow<PetRef> @JvmName("Error_petRef") get() = this["petRef"] as org.jetbrains.kotlinx.dataframe.DataRow<PetRef>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.petRef: org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<PetRef?> @JvmName("NullableError_petRef") get() = this["petRef"] as org.jetbrains.kotlinx.dataframe.columns.ColumnGroup<PetRef?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.petRef: org.jetbrains.kotlinx.dataframe.DataRow<PetRef?> @JvmName("NullableError_petRef") get() = this["petRef"] as org.jetbrains.kotlinx.dataframe.DataRow<PetRef?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error>.pets: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>> @JvmName("Error_pets") get() = this["pets"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error>.pets: org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?> @JvmName("Error_pets") get() = this["pets"] as org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.Error?>.pets: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>> @JvmName("NullableError_pets") get() = this["pets"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.Error?>.pets: org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?> @JvmName("NullableError_pets") get() = this["pets"] as org.jetbrains.kotlinx.dataframe.DataFrame<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional2>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any> @JvmName("ObjectWithAdditional2_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional2>.`value`: kotlin.Any @JvmName("ObjectWithAdditional2_value") get() = this["value"] as kotlin.Any
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional2?>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("NullableObjectWithAdditional2_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional2?>.`value`: kotlin.Any? @JvmName("NullableObjectWithAdditional2_value") get() = this["value"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional2>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("ObjectWithAdditional2_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional2>.key: kotlin.String @JvmName("ObjectWithAdditional2_key") get() = this["key"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional2?>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableObjectWithAdditional2_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional2?>.key: kotlin.String? @JvmName("NullableObjectWithAdditional2_key") get() = this["key"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional3>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("ObjectWithAdditional3_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional3>.`value`: kotlin.Any? @JvmName("ObjectWithAdditional3_value") get() = this["value"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional3?>.`value`: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?> @JvmName("NullableObjectWithAdditional3_value") get() = this["value"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.Any?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional3?>.`value`: kotlin.Any? @JvmName("NullableObjectWithAdditional3_value") get() = this["value"] as kotlin.Any?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional3>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String> @JvmName("ObjectWithAdditional3_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional3>.key: kotlin.String @JvmName("ObjectWithAdditional3_key") get() = this["key"] as kotlin.String
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ObjectWithAdditional3?>.key: org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?> @JvmName("NullableObjectWithAdditional3_key") get() = this["key"] as org.jetbrains.kotlinx.dataframe.DataColumn<kotlin.String?>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ObjectWithAdditional3?>.key: kotlin.String? @JvmName("NullableObjectWithAdditional3_key") get() = this["key"] as kotlin.String?
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ErrorHolder>.errors: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error>> @JvmName("ErrorHolder_errors") get() = this["errors"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ErrorHolder>.errors: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error> @JvmName("ErrorHolder_errors") get() = this["errors"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error>
            val org.jetbrains.kotlinx.dataframe.ColumnsContainer<`Other advanced test`.ErrorHolder?>.errors: org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error?>> @JvmName("NullableErrorHolder_errors") get() = this["errors"] as org.jetbrains.kotlinx.dataframe.DataColumn<org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error?>>
            val org.jetbrains.kotlinx.dataframe.DataRow<`Other advanced test`.ErrorHolder?>.errors: org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error?> @JvmName("NullableErrorHolder_errors") get() = this["errors"] as org.jetbrains.kotlinx.dataframe.DataFrame<`Other advanced test`.Error?>
        """.trimLines()

        code shouldBe s
    }

    @Test
    fun `Other advanced test`() {
        val fullFunctionName = ValidFieldName.of(::`Other advanced test`.name)
        val functionName = fullFunctionName.quotedIfNeeded
        val code = execGeneratedCode(advancedExample, fullFunctionName.unquoted)
            .trimLines()

        @Language("kt")
        val res1 = execRaw(
            "$functionName.Pet.readJsonStr(\"\"\"$advancedData\"\"\").filter { petType == \"Cat\" }.convertTo<$functionName.Cat>(ExcessiveColumns.Remove)"
        ) as AnyFrame
        val res1Schema = res1.schema()

        @Language("kts")
        val res2 = execRaw(
            "$functionName.Pet.readJsonStr(\"\"\"$advancedData\"\"\").filter { petType == \"Dog\" }.convertTo<$functionName.Dog>(ExcessiveColumns.Remove)"
        ) as AnyFrame
        val res2Schema = res2.schema()

        @Language("kts")
        val res3 = execRaw(
            "$functionName.Error.readJsonStr(\"\"\"$advancedErrorData\"\"\")"
        ) as AnyFrame
        val res3Schema = res3.schema()

        @Language("kts")
        val res4 = execRaw(
            "$functionName.ErrorHolder.readJsonStr(\"\"\"$advancedErrorHolderData\"\"\")"
        ) as AnyFrame
        val res4Schema = res4.schema()
    }

    @Test
    fun `Apis guru Test`() {
        val fullFunctionName = ValidFieldName.of(::`Apis guru Test`.name)
        val functionName = fullFunctionName.quotedIfNeeded
        val code = execGeneratedCode(apiGuruYaml, fullFunctionName.unquoted)

        val apiGuruDataTripleQuote = "\"\"\"${apiGuruData.replace("$", "\${'$'}")}\"\"\""

        @Language("kts")
        val df = execRaw(
            """
            $functionName.APIs.readJsonStr($apiGuruDataTripleQuote)
                .filter {
                    value.versions.value.any {
                        (updated ?: added).year > 2019
                    }
                }
            """.trimIndent()
        ) as AnyFrame

        df.isNotEmpty().shouldBeTrue()
    }

    @Test
    fun `MLC Test 1`() {
        val code = execGeneratedCode(mlcYaml, ::`MLC Test 1`.name)
        println(code)

        val mlcLocationsWithPeopleDataTripleQuote = "\"\"\"${mlcLocationsWithPeopleData.replace("$", "\${'$'}")}\"\"\""

        @Language("kts")
        val df1 = execRaw(
            """`${::`MLC Test 1`.name}`.LocationsWithPeople.readJsonStr($mlcLocationsWithPeopleDataTripleQuote)
                    .also { it.print() }
            """.trimIndent()
        ) as AnyFrame

        df1.isNotEmpty().shouldBeTrue()
    }

    @Test
    fun `MLC Test 2`() {
        val code = execGeneratedCode(mlcYaml, ::`MLC Test 2`.name)
        println(code)

        val mlcPeopleWithLocationDataTripleQuote = "\"\"\"${mlcPeopleWithLocationData.replace("$", "\${'$'}")}\"\"\""

        @Language("kts")
        val df2 = execRaw(
            """`${::`MLC Test 2`.name}`.PeopleWithLocation.readJsonStr($mlcPeopleWithLocationDataTripleQuote)
                    .also { it.print() }
            """.trimIndent()
        ) as AnyFrame

        df2.isNotEmpty().shouldBeTrue()
    }

    @Test
    fun `Jupyter importDataSchema`() {
        val filePath = apiGuruYaml.absolutePath.let {
            if (separatorChar == '\\') it.replace("\\", "\\\\")
            else it
        }
        @Language("kt")
        val _1 = execRaw(
            """
                val ApiGuru = importDataSchema(File("$filePath"))
            """.trimIndent()
        )

        val apiGuruDataTripleQuote = "\"\"\"${apiGuruData.replace("$", "\${'$'}")}\"\"\""

        @Language("kt")
        val _2 = execRaw(
            """
                val df = ApiGuru.APIs.readJsonStr($apiGuruDataTripleQuote)
                df
            """.trimIndent()
        ) as AnyFrame

        println(_2)

        @Language("kt")
        val _3 = execRaw(
            """
                df.filter {
                  value.versions.value.any {
                    (updated ?: added).year >= 2021
                  }
                }
            """.trimIndent()
        ) as AnyFrame

        @Language("kt")
        val _4 = execRaw(
            """
                ApiGuru.APIs.readJsonStr($apiGuruDataTripleQuote).filter {
                  value.versions.value.any {
                    (updated ?: added).year >= 2021
                  }
                }
            """.trimIndent()
        ) as AnyFrame
    }

    private fun String.trimLines(): String = trim().removeSurrounding("\n").lines().joinToString("\n") { it.trim() }
}
