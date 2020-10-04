package com.stevebowser.enduranceactivityfileanalyser

import java.util.Properties

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}
import com.stevebowser.enduranceactivityfileanalyser.analysis.PersonalBestAnalyser.{calculateDistancePersonalBests, calculateSensorPersonalBests}
import scala.io.Source

object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val testActivityDataset : Dataset[FileParser.ActivityRecord] = FileParser.readGPXToDataFrame("Data/", spark)

    testActivityDataset.show

    val personalBests = calculateDistancePersonalBests(testActivityDataset, 5)

    personalBests.show

    val sensorBests = calculateSensorPersonalBests(testActivityDataset, 600L)

    sensorBests.show()

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
