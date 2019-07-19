package com.simplilearn.bigdata.casestudy_11

import org.apache.spark.ml.{Pipeline, PipelineStage}
import org.apache.spark.ml.linalg.{Vector}
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.ml.feature.{OneHotEncoder, StringIndexer, VectorAssembler, VectorIndexer}
import org.apache.spark.ml.regression.GBTRegressor
import org.apache.spark.sql.functions._
import scala.collection.mutable.ListBuffer

object Solution_1 {

  def main(args: Array[String]): Unit = {

    if (args.length != 2) {
      System.out.println("Please provide <input_path> <spark_master>")
      System.exit(0)
    }
    val inputPath: String = args(0)

    val dataset = readFile(inputPath, readWithHeader(getSparkSession("diamond-prediction", args(1))))

    //    modelling(dataset)

    //    modellingUsingOHE(dataset)

    //    modellingUsingOHEWithDense(dataset)

    modellingVectorIndexer(dataset)
  }

  def modellingVectorIndexer(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.drop("index")
    modifiedDataset.select("cut").groupBy("cut").count().show();
    modifiedDataset.select("color").groupBy("color").count().show()
    modifiedDataset.select("clarity").groupBy("clarity").count().show()
    System.out.println(modifiedDataset.describe().show())

    val features = List("color", "cut", "clarity")

    val encodedFeatures = features.flatMap {
      name => {
        val indexer =
          new StringIndexer()
            .setInputCol(name)
            .setOutputCol(name + "_index")
        Array(indexer)
      }
    }.toArray


    modifiedDataset =
      new Pipeline()
        .setStages(encodedFeatures)
        .fit(modifiedDataset)
        .transform(modifiedDataset)
        .drop("color", "cut", "clarity");

    System.out.println(modifiedDataset.describe().show())

    val assembler = new VectorAssembler()
      .setInputCols(Array("carot","depth","table","price","x","y","z","color_index","cut_index","clarity_index"))
      .setOutputCol("features")

    modifiedDataset =
      new Pipeline()
        .setStages(Array(assembler))
        .fit(modifiedDataset)
        .transform(modifiedDataset)
        .drop("carot","depth","table","x","y","z","color_index","cut_index","clarity_index");

    System.out.println("Dataset pre vector indexer \n"+modifiedDataset.show(10, false))

    val vectorIndexer = new VectorIndexer()
      .setInputCol("features")
      .setOutputCol("indexed_features")
      .setMaxCategories(10)

    println("Categorical features = "+vectorIndexer.fit(modifiedDataset).transform(modifiedDataset).show(10))

    modifiedDataset =
      new Pipeline()
        .setStages(Array(vectorIndexer))
        .fit(modifiedDataset)
        .transform(modifiedDataset)
        .drop("cut", "color", "clarity", "carot","depth","table","x","y","z","color_index","cut_index","clarity_index", "features");

    System.out.println("Dataset post vector indexer \n"+modifiedDataset.show(10, false))

    val Array(trainingData, testData) = modifiedDataset.randomSplit(Array(0.8, 0.2))

    val labelColumn = "price"
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("indexed_features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    val model = new Pipeline().setStages(Array(gbt)).fit(trainingData)

    val predictions = model.transform(testData)

    val pricePercentageDiff = udf((price: Float, predictedPrice: Float) => {
      val percentageDiff = (Math.abs(predictedPrice - price) / price) * 100
      if(percentageDiff > 5)  "Not Predicted Correctly" else "Predicted Correctly"
    })

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)

    println("Test data Error : "+error)

    println("Train data Error : "+evaluator.evaluate(model.transform(trainingData)))

    val lowOrHighPricePrediction = udf((price: Float, predictedPrice: Float) => {
      if(predictedPrice - price > 0)  "High" else if(predictedPrice - price < 0) "Low" else "Equal"
    })


    println("Prediction data on the basis of difference between Actual and Predicted price by 5% = "+predictions
      .select("price", "Predicted price")
      .withColumn("Predicted", pricePercentageDiff(predictions("price"),predictions("Predicted price")))
      .groupBy("Predicted").count().show(2)
    )

    println("Prediction data stats on the basis if predicted price was higher or lower = "+predictions
      .select("price", "Predicted price")
      .withColumn("Predicted", lowOrHighPricePrediction(predictions("price"),predictions("Predicted price")))
      .groupBy("Predicted").count().show(3)
    )
  }


  def modellingUsingOHEWithDense(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.drop("index")
    modifiedDataset.select("cut").groupBy("cut").count().show();
    modifiedDataset.select("color").groupBy("color").count().show()
    modifiedDataset.select("clarity").groupBy("clarity").count().show()
    System.out.println(modifiedDataset.describe().show())

    val features = List("color", "cut", "clarity")

    var stages = new ListBuffer[PipelineStage]()
    var i = 0
    for (i <- 0 to features.size - 1) {
      val name = features(i)
      val indexer =
        new StringIndexer()
          .setInputCol(name)
          .setOutputCol(name + "_index")
      val encoder = new OneHotEncoder()
        .setInputCol(indexer.getOutputCol)
        .setOutputCol(name +"_vector")
      stages += indexer
      stages += encoder
    }


    modifiedDataset =
      new Pipeline()
        .setStages(stages.toArray)
        .fit(modifiedDataset)
        .transform(modifiedDataset)
        .drop("color", "cut", "clarity");

    val sparseToDense = udf((v: Vector) => v.toDense)

    System.out.println("Dataset one hot encoded applied \n"+modifiedDataset.show(10, false))

    modifiedDataset =
      modifiedDataset
        .withColumn("color_vector_dense", sparseToDense(modifiedDataset("color_vector")))
        .withColumn("cut_vector_dense", sparseToDense(modifiedDataset("cut_vector")))
        .withColumn("clarity_vector_dense", sparseToDense(modifiedDataset("clarity_vector")))
        .drop("color_vector", "cut_vector", "clarity_vector")


    System.out.println("Dataset one hot encoded applied and converted to Dense \n"+modifiedDataset.show(10, false))

    val Array(trainingData, testData) = modifiedDataset.randomSplit(Array(0.8, 0.2))

    val assembler = new VectorAssembler()
      .setInputCols(Array("carot","depth","table","x","y","z","color_index","cut_index","clarity_index"))
      .setOutputCol("features")

    val labelColumn = "price"
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    val model = new Pipeline().setStages(Array(assembler, gbt)).fit(trainingData)

    val testPredictions = model.transform(testData)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(testPredictions)

    println("Test data Error : "+error)

    val trainPredictions = model.transform(trainingData)
    println("Train data Error : "+evaluator.evaluate(trainPredictions))
  }

  def modellingUsingOHE(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.drop("index")
    modifiedDataset.select("cut").groupBy("cut").count().show();
    modifiedDataset.select("color").groupBy("color").count().show()
    modifiedDataset.select("clarity").groupBy("clarity").count().show()
    System.out.println(modifiedDataset.describe().show())

    val features = List("color", "cut", "clarity")

    var stages = new ListBuffer[PipelineStage]()
    var i = 0
    for (i <- 0 to features.size - 1) {
      val name = features(i)
      val indexer =
        new StringIndexer()
          .setInputCol(name)
          .setOutputCol(name + "_index")
      val encoder = new OneHotEncoder()
        .setInputCol(indexer.getOutputCol)
        .setOutputCol(name +"_vector")
      stages += indexer
      stages += encoder
    }

    modifiedDataset =
      new Pipeline()
        .setStages(stages.toArray)
        .fit(modifiedDataset)
        .transform(modifiedDataset)
        .drop("color", "cut", "clarity");

    System.out.println("Dataset one hot encoded applied \n"+modifiedDataset.show(10, false))

    val Array(trainingData, testData) = modifiedDataset.randomSplit(Array(0.8, 0.2))

    val assembler = new VectorAssembler()
      .setInputCols(Array("carot","depth","table","price","x","y","z","color_index","cut_index","clarity_index"))
      .setOutputCol("features")

    val labelColumn = "price"
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    val model = new Pipeline().setStages(Array(assembler, gbt)).fit(trainingData)

    val predictions = model.transform(testData)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)

    println("Test data Error : "+error)

    println("Train data Error : "+evaluator.evaluate(model.transform(trainingData)))
  }

  def modelling(dataset: Dataset[Row]): Unit = {
    var modifiedDataset = dataset.drop("index")
    modifiedDataset.select("cut").groupBy("cut").count().show();
    modifiedDataset.select("color").groupBy("color").count().show()
    modifiedDataset.select("clarity").groupBy("clarity").count().show()
    System.out.println(modifiedDataset.describe().show())

    val features = List("color", "cut", "clarity")

    val encodedFeatures = features.flatMap {
      name => {
        val indexer =
          new StringIndexer()
            .setInputCol(name)
            .setOutputCol(name + "_index")
        Array(indexer)
      }
    }.toArray

    modifiedDataset =
      new Pipeline()
        .setStages(encodedFeatures)
        .fit(modifiedDataset)
        .transform(modifiedDataset);

    System.out.println(modifiedDataset.describe().show())

    val Array(trainingData, testData) = modifiedDataset.randomSplit(Array(0.8, 0.2))


    val assembler = new VectorAssembler()
      .setInputCols(Array("carot","depth","table","price","x","y","z","color_index","cut_index","clarity_index"))
      .setOutputCol("features")

    val labelColumn = "price"
    val gbt = new GBTRegressor()
      .setLabelCol(labelColumn)
      .setFeaturesCol("features")
      .setPredictionCol("Predicted " + labelColumn)
      .setMaxIter(50)

    val model = new Pipeline().setStages(Array(assembler, gbt)).fit(trainingData)

    val predictions = model.transform(testData)

    val evaluator = new RegressionEvaluator()
      .setLabelCol(labelColumn)
      .setPredictionCol("Predicted " + labelColumn)
      .setMetricName("rmse")

    //We compute the error using the evaluator
    val error = evaluator.evaluate(predictions)

    println("Test data Error : "+error)

    println("Train data Error : "+evaluator.evaluate(model.transform(trainingData)))
  }

  def getSparkSession(appName: String, master: String) = {
    val sparkSession = SparkSession.builder.appName(appName).master(if (master.equalsIgnoreCase("local")) "local[*]"
    else master).getOrCreate
    System.out.println("Spark version " + sparkSession.version)
    sparkSession
  }

  def readFile(path: String, dataFrameReader: DataFrameReader) = {
    System.out.println("Reading file " + path)
    val dataset = dataFrameReader.csv(path)
    System.out.println("Dataset Schema " + dataset.schema)
    System.out.println("Row Count" + dataset.count())
    dataset
  }

  def readWithHeader(sparkSession: SparkSession) = {
    val transactionSchema = StructType(Array(
      StructField("index", IntegerType, true),
      StructField("carot", FloatType, true),
      StructField("cut", StringType, true),
      StructField("color", StringType, true),
      StructField("clarity", StringType, true),
      StructField("depth", FloatType, true),
      StructField("table", FloatType, true),
      StructField("price", FloatType, true),
      StructField("x", FloatType, true),
      StructField("y", FloatType, true),
      StructField("z", FloatType, true)
    ))
    sparkSession.read.option("header", true).schema(transactionSchema).option("mode", "DROPMALFORMED")
  }
}
