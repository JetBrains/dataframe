package org.jetbrains.kotlinx.dataframe.api

import org.jetbrains.kotlinx.dataframe.AnyFrame
import org.jetbrains.kotlinx.dataframe.DataFrame
import org.jetbrains.kotlinx.dataframe.impl.api.toSequenceImpl
import kotlin.reflect.typeOf

// region DataFrame

public inline fun <reified T> DataFrame<T>.toSequence(): Sequence<T> = toSequenceImpl(typeOf<T>()) as Sequence<T>

public inline fun <reified T> AnyFrame.toSequenceOf(): Sequence<T> = toSequenceImpl(typeOf<T>()) as Sequence<T>

// endregion
