package com.stevebowser.enduranceactivityfileanalyser.analysis
import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}



object RegressionModel {

  def runLinearRegression (df: DataFrame, inputCols : Array[String], outputCol : String) : Unit = {

    val linearRegData = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")
      .transform(df)
      .cache()


    val lr = new LinearRegression()
      .setMaxIter(10)
      .setFeaturesCol("features")   // setting features column
      .setLabelCol(outputCol)


    // Fit the model
    val lrModel = lr.fit(linearRegData)

    //creating pipeline

    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${lrModel.coefficients} Intercept: ${lrModel.intercept}")

    // Summarize the model over the training set and print out some metrics
    //val trainingSummary = lrModel.summary
    //println(s"numIterations: ${trainingSummary.totalIterations}")
    //println(s"objectiveHistory: [${trainingSummary.objectiveHistory.mkString(",")}]")
    //trainingSummary.residuals.show()
    //println(s"RMSE: ${trainingSummary.rootMeanSquaredError}")
    //println(s"r2: ${trainingSummary.r2}")


  }

}
