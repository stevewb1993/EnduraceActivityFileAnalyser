package com.stevebowser.enduranceactivityfileanalyser.analysis
import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.sql._
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegressionModel
import org.apache.spark.sql.functions._




object RegressionModel {

  def runLinearRegression (ds: Dataset[ActivityRecord], inputCols : Array[String], outputCol : String, activityType : String) : LinearRegressionModel = {

    val linearRegData = new VectorAssembler()
      .setInputCols(inputCols)
      .setOutputCol("features")
      .setHandleInvalid("skip")
      .transform(
        ds.where(col(outputCol).isNotNull)
          .filter(r => r.parsedActivityType == activityType)
      )

    val lr = new LinearRegression()
      .setMaxIter(10)
      .setFeaturesCol("features")   // setting features column
      .setLabelCol(outputCol)

    // Fit the model
    lr.fit(linearRegData)

  }

}
