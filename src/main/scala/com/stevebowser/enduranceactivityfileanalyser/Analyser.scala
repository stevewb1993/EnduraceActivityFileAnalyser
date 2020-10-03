package com.stevebowser.enduranceactivityfileanalyser

import org.apache.spark.sql.{Dataset, SparkSession}

object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GPXAnalyser")
      .master("local[3]")
      .getOrCreate()

    val testActivityDataset : Dataset[ActivityFileParser.ActivityRecord] = ActivityFileParser.readGPXToDataFrame("Data/activity_4900763877.gpx", spark)

    testActivityDataset.show


  }
}
