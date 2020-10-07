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

    val testActivityDataset : Dataset[ActivityRecord]  = FileParser.readGPXToDataFrame("Data/", spark)

    testActivityDataset.show(100)

    //val personalBests = calculateDistancePersonalBests(testActivityDataset, 5)
//
    //personalBests.show
//
    //val sensorBests = calculateSensorPersonalBests(testActivityDataset, 600L)
//
    //sensorBests.show()


    val trainingData= testActivityDataset
      .withColumn("heartRate2", col("heartRate").cast(DoubleType))
      .withColumn("cadence2", col("cadence").cast(DoubleType))

    val inputCols : Array[String] = Array("heartRate2", "cadence2")
    val outputcolumn = "smoothSpeedKmH"
    RegressionModel.runLinearRegression(trainingData,inputCols,outputcolumn)

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
