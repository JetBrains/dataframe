package org.jetbrains.kotlinx.dataframe.annotations

@Target(AnnotationTarget.CLASS)
public annotation class HasSchema(val schemaArg: Int)

/**
 * Compiler plugin will evaluate compile time value of the annotated function.
 * Needed because some function calls only serve as a part of overall compile time DataSchema evaluation
 * There's no need to update return type of such calls
 */
public annotation class Interpretable(val interpreter: String)

/**
 * Compiler plugin will replace return type of calls to the annotated function
 */
public annotation class Refine

internal annotation class OptInRefine

@Retention(AnnotationRetention.SOURCE)
@Target(AnnotationTarget.FILE, AnnotationTarget.EXPRESSION)
public annotation class DisableInterpretation

@Retention(AnnotationRetention.SOURCE)
@Target(AnnotationTarget.EXPRESSION)
public annotation class Import

@Target(AnnotationTarget.PROPERTY)
public annotation class Order(val order: Int)

/**
 * For internal use
 * Compiler plugin materializes schemas as classes.
 * These classes have two kinds of properties:
 * 1. Scope properties that only serve as a reference for internal property resolution
 * 2. Schema properties that reflect dataframe structure
 * Scope properties need
 * to be excluded in IDE plugin and in [org.jetbrains.kotlinx.dataframe.codeGen.MarkersExtractor.get]
 * This annotation serves to distinguish between the two where needed
 */
@Target(AnnotationTarget.PROPERTY)
public annotation class ScopeProperty

@Target(AnnotationTarget.FUNCTION)
internal annotation class Check
