package com.stevebowser.enduranceactivityfileanalyser

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser
import org.apache.spark.sql.{Dataset, SparkSession}

object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GPXAnalyser")
      .master("local[3]")
      .getOrCreate()

    val testActivityDataset : Dataset[FileParser.ActivityRecord] = FileParser.readGPXToDataFrame("Data/activity_4900763877.gpx", spark)

    testActivityDataset.show

    spark.stop()

  }
}
