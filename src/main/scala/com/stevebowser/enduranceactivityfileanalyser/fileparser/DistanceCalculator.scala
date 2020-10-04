package com.stevebowser.enduranceactivityfileanalyser.fileparser
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataType

private object DistanceCalculator {

  //add the lag of GPS Position for calculating distance between each track point
  private def addLagTimeAndPosition(df: DataFrame) : DataFrame = {
    val previousTrackPointWindow = Window.partitionBy("activityId").orderBy("activityTrackPoint")
    df
      .withColumn("lagLongitude", lag(col("longitude"), 1).over(previousTrackPointWindow))
      .withColumn("lagLatitude", lag(col("latitude"), 1).over(previousTrackPointWindow))
      .withColumn("lagTime", lag(col("time"), 1).over(previousTrackPointWindow))
  }

  //adapted from https://gist.github.com/pavlov99/bd265be244f8a84e291e96c5656ceb5c
  //used for calculating distance between two GPS latitude and longitude points
  private def addDistanceFromLastPoint (df: DataFrame) : DataFrame = {
    //intermediary calculation of distance
    df.withColumn("a", org.apache.spark.sql.functions.pow(sin(radians(col("latitude") - col("lagLatitude")) / 2), 2) + cos(radians(col("lagLatitude"))) * cos(radians(col("latitude"))) * org.apache.spark.sql.functions.pow(sin(radians(col("longitude") - col("lagLongitude")) / 2), 2))
      //final distance calculation
      .withColumn("distanceFromLastPoint", org.apache.spark.sql.functions.atan2(org.apache.spark.sql.functions.sqrt(col("a")), org.apache.spark.sql.functions.sqrt(-col("a") + 1)) * 2 * 6371)
      .drop("a")
  }

  //used for calculating time between points
  private def addTimeFromLastPoint (df: DataFrame) : DataFrame = {
    //intermediary calculation of distance
    df.withColumn("timeFromLastPoint", df("time").cast("long") - df("lagTime").cast("long"))
  }


  def addCumulativeStatistics (df: DataFrame) : DataFrame = {
    //get the previous time and position for calculating intermidatry time and distance
    val addPreviousTimeAndDistanceDf = addLagTimeAndPosition(df)
    //use the previous time and position for calculating changes
    val addChangesInTimeAndDistanceDf = addTimeFromLastPoint(addDistanceFromLastPoint(addPreviousTimeAndDistanceDf))
    //sum the changes in time and position from the start of the activity to the current point in order to calculate cumulative statistics
    val startToCurrentPointWindow = Window.partitionBy("activityId").orderBy("activityTrackPoint").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    addChangesInTimeAndDistanceDf
      .withColumn("cumulativeDistanceKm", sum("distanceFromLastPoint").over(startToCurrentPointWindow))
      .withColumn("cumulativeTime", sum("timeFromLastPoint").over(startToCurrentPointWindow))
      //remove previous columns used for calculation
      .drop("distanceFromLastPoint")
      .drop("timeFromLastPoint")
      .drop("lagLongitude")
      .drop("lagLatitude")
      .drop("lagTime")
  }
}
