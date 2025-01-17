//package org.jetbrains.kotlinx.dataframe.examples.titanic.ml
//
//import org.jetbrains.kotlinx.dataframe.ColumnSelector
//import org.jetbrains.kotlinx.dataframe.DataFrame
//import org.jetbrains.kotlinx.dataframe.api.*
//import org.jetbrains.kotlinx.dl.api.core.Sequential
//import org.jetbrains.kotlinx.dl.api.core.activation.Activations
//import org.jetbrains.kotlinx.dl.api.core.initializer.HeNormal
//import org.jetbrains.kotlinx.dl.api.core.initializer.Zeros
//import org.jetbrains.kotlinx.dl.api.core.layer.core.Dense
//import org.jetbrains.kotlinx.dl.api.core.layer.core.Input
//import org.jetbrains.kotlinx.dl.api.core.loss.Losses
//import org.jetbrains.kotlinx.dl.api.core.metric.Metrics
//import org.jetbrains.kotlinx.dl.api.core.optimizer.Adam
//import org.jetbrains.kotlinx.dl.dataset.OnHeapDataset
//import java.util.Locale
//
//private const val SEED = 12L
//private const val TEST_BATCH_SIZE = 100
//private const val EPOCHS = 50
//private const val TRAINING_BATCH_SIZE = 50
//
//private val model = Sequential.of(
//    Input(9),
//    Dense(50, Activations.Relu, kernelInitializer = HeNormal(SEED), biasInitializer = Zeros()),
//    Dense(50, Activations.Relu, kernelInitializer = HeNormal(SEED), biasInitializer = Zeros()),
//    Dense(2, Activations.Linear, kernelInitializer = HeNormal(SEED), biasInitializer = Zeros())
//)
//
//fun main() {
//
//    // Set Locale for correct number parsing
//    Locale.setDefault(Locale.FRANCE)
//
//    val df = Passenger.readCSV()
//
//    // Calculating imputing values
//    val (train, test) = df
//        // imputing
//        .fillNulls { sibsp and parch and age and fare }.perCol { it.mean() }
//        .fillNulls { sex }.with { "female" }
//        // one hot encoding
//        .pivotMatches { pclass and sex }
//        // feature extraction
//        .select { survived and pclass and sibsp and parch and age and fare and sex }
//        .shuffle()
//        .toTrainTest(0.7) { survived }
//
//    model.use {
//        it.compile(
//            optimizer = Adam(),
//            loss = Losses.SOFT_MAX_CROSS_ENTROPY_WITH_LOGITS,
//            metric = Metrics.ACCURACY
//        )
//
//        it.summary()
//        it.fit(dataset = train, epochs = EPOCHS, batchSize = TRAINING_BATCH_SIZE)
//
//        val accuracy = it.evaluate(dataset = test, batchSize = TEST_BATCH_SIZE).metrics[Metrics.ACCURACY]
//
//        println("Accuracy: $accuracy")
//    }
//}
//
//fun <T> DataFrame<T>.toTrainTest(
//    trainRatio: Double,
//    yColumn: ColumnSelector<T, Number>,
//): Pair<OnHeapDataset, OnHeapDataset> =
//    toOnHeapDataset(yColumn)
//        .split(trainRatio)
//
//private fun <T> DataFrame<T>.toOnHeapDataset(yColumn: ColumnSelector<T, Number>): OnHeapDataset =
//    OnHeapDataset.create(
//        dataframe = this,
//        yColumn = yColumn,
//    )
//
//private fun <T> OnHeapDataset.Companion.create(
//    dataframe: DataFrame<T>,
//    yColumn: ColumnSelector<T, Number>,
//): OnHeapDataset {
//    val x by column<FloatArray>("X")
//
//    fun extractX(): Array<FloatArray> =
//        dataframe.remove(yColumn)
//            .convert { colsAtAnyDepth { !it.isColumnGroup() } }.toFloat()
//            .merge { colsAtAnyDepth().colsOf<Float>() }.by { it.toFloatArray() }.into(x)
//            .getColumn(x).toTypedArray()
//
//    fun extractY(): FloatArray = dataframe.get(yColumn).toFloatArray()
//
//    return create(
//        ::extractX,
//        ::extractY,
//    )
//}
