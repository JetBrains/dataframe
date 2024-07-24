package org.jetbrains.kotlinx.dataframe.codeGen

import org.jetbrains.kotlinx.dataframe.impl.codeGen.ExtensionsCodeGeneratorImpl
import org.jetbrains.kotlinx.dataframe.impl.codeGen.ShortNames

public interface ExtensionsCodeGenerator {
    public fun generate(marker: IsolatedMarker): CodeWithConverter<*>

    public companion object {
        public fun create(): ExtensionsCodeGenerator = ExtensionsCodeGeneratorImpl(ShortNames)
    }
}
