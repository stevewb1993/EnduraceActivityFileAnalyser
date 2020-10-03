package com.stevebowser.enduranceactivityfileanalyser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import scala.math._

object DistanceCalculator {

  //add the lag of GPS Position for calculating distance between each track point
  def addLagTimePosition (df: DataFrame) : DataFrame = {
    val previousTrackPointWindow = Window.partitionBy("activityId").orderBy("activityTrackPoint")
    df.withColumn("lagLongitude", lag(col("longitude"), 1).over(previousTrackPointWindow))
    df.withColumn("lagLatitude", lag(col("latitude"), 1).over(previousTrackPointWindow))
    df.withColumn("lagTime", lag(col("time"), 1).over(previousTrackPointWindow))
  }

  //adapted from https://gist.github.com/pavlov99/bd265be244f8a84e291e96c5656ceb5c
  def addDistanceFromLastPoint (df: DataFrame) : DataFrame = {
    //intermediary calculation of distance
    df.withColumn("a", pow(sin(radians(col("latitude") - col("lagLatitude")) / 2), 2) + cos(radians(col("lagLatitude"))) * cos(radians(col("latitude"))) * pow(sin(radians(col("longitude") - col("lagLongitude")) / 2), 2))
      //final distance calculation
      .withColumn("distanceFromLastPoint", atan2(sqrt(col("a")), sqrt(-col("a") + 1)) * 2 * 6371)
      .drop("a")
  }

  def addTimeFromLastPoint (df: DataFrame) : DataFrame = {
    //intermediary calculation of distance
    df.withColumn("timeFromLastPoint", df("time") - df("lagTime"))
  }


  def addCumulativeStatistics (df: DataFrame) : DataFrame = {
    val startToCurrentPointWindow = Window.partitionBy("activityId").orderBy("activityTrackPoint").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    addTimeFromLastPoint(addDistanceFromLastPoint(df))
      .withColumn("cumulativeDistanceKm", sum("distanceFromLastPoint").over(startToCurrentPointWindow))
      .withColumn("cumulativeTime", sum("timeFromLastPoint").over(startToCurrentPointWindow))
      .drop("distanceFromLastPoint")
  }
}
