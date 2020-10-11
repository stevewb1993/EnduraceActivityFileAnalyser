package com.stevebowser.enduranceactivityfileanalyser

import java.util.Properties

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import com.stevebowser.enduranceactivityfileanalyser.analysis.PersonalBestAnalyser.{calculateDistancePersonalBests, calculateSensorPersonalBests}
import com.stevebowser.enduranceactivityfileanalyser.analysis.RegressionModel
import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DoubleType, StringType}

import scala.io.Source

object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    //read data
    val testActivityDataset : Dataset[ActivityRecord]  = FileParser.readGPXToDataFrame("Data/", spark)

    //show personal bests over any given distance
    val personalBests = calculateDistancePersonalBests(testActivityDataset, 5)
    personalBests.show

    //show best performances for all sensor types over a given time interval
    val sensorBests = calculateSensorPersonalBests(testActivityDataset, 600L)
    sensorBests.show()

    //run regression analysis
    val inputCols : Array[String] = Array("heartRate", "cadence", "gradient")
    val outputColumn = "smoothSpeedKmH"
    val activityType = "cycle"
    val regressionModel = RegressionModel.runLinearRegression(testActivityDataset, inputCols, outputColumn, activityType)
    // Print the coefficients and intercept for linear regression
    println(s"Coefficients: ${regressionModel.coefficients} Intercept: ${regressionModel.intercept}")
    println(s"r2: ${regressionModel.summary.r2}")

    spark.stop()

  }

  def getSparkAppConf: SparkConf = {
    val sparkAppConf = new SparkConf
    //Set all Spark Configs
    val props = new Properties
    props.load(Source.fromFile("spark.conf").bufferedReader())
    props.forEach((k, v) => sparkAppConf.set(k.toString, v.toString))
    sparkAppConf
  }

}
