package org.jetbrains.kotlinx.dataframe.codeGen

import org.jetbrains.kotlinx.jupyter.api.Code
import org.jetbrains.kotlinx.jupyter.api.VariableName

/**
 * Class representing generated code declarations for a [Marker].
 *
 * @param declarations The generated code.
 * @param converter Needs to provide additional info (name) from org.jetbrains.dataframe.impl.codeGen.CodeGenerator to its callers
 * But at the same time name doesn't make sense for GroupBy where code to be executed contains two declarations
 * @param converter Optional converter for the [Marker], such as a [org.jetbrains.kotlinx.dataframe.api.cast], often used for Jupyter.
 */
public data class CodeWithConverter<T : CodeConverter>(
    val declarations: Code,
    val converter: T
) {

    public companion object {
        public const val EmptyDeclarations: Code = ""
        public val EmptyConverter: CodeConverter = CodeConverter { it }
        public val Empty: CodeWithConverter<CodeConverter> = CodeWithConverter(EmptyDeclarations, EmptyConverter)
    }

    val hasDeclarations: Boolean get() = declarations.isNotBlank()

    val hasConverter: Boolean get() = converter("it").trim() != "it"

    public fun with(name: VariableName): Code = when {
        !hasConverter -> declarations
        !hasDeclarations -> converter(name)
        else -> declarations + "\n" + converter(name)
    }
}

public sealed interface CodeConverter : (VariableName) -> Code

public class CodeConverterImpl(private val f: (VariableName) -> Code) : CodeConverter {
    override fun invoke(p1: VariableName): Code {
        return f(p1)
    }
}

public fun CodeConverter(f: (VariableName) -> Code): CodeConverter = CodeConverterImpl(f)

public class ProvidedCodeConverter(public val markerName: String) : CodeConverter {
    override fun invoke(p1: VariableName): Code {
        return "$p1.cast<$markerName>()"
    }
}
