package com.stevebowser.enduranceactivityfileanalyser

import java.util.Properties

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.io.Source

object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .config(getSparkAppConf)
      .getOrCreate()

    val testActivityDataset : Dataset[FileParser.ActivityRecord] = FileParser.readGPXToDataFrame("Data/activity_4900763877.gpx", spark)

    testActivityDataset.show

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
