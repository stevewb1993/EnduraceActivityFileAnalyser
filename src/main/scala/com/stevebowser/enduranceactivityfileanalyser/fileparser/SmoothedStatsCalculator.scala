package com.stevebowser.enduranceactivityfileanalyser.fileparser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SmoothedStatsCalculator {

  def addSmoothedDistanceElevation(df : DataFrame, smoothingFactor: Int): DataFrame = {

    //window for calculating speed. Look +- the smoothingFactor
    val rollingTimeWindow = Window.partitionBy("activityId").orderBy("cumulativeTime").rangeBetween(-smoothingFactor, smoothingFactor)
    df
      //calculate time change over time window (cannot assume the smooth factor due to edge cases). change into hours to help with units
      .withColumn("startTime", min("cumulativeTime") over rollingTimeWindow)
      .withColumn("endTime", max("cumulativeTime") over rollingTimeWindow)
      .withColumn("timeChange", col("endTime") - col("startTime"))
        //change timeChange to hours to ensure correct units
      .withColumn("timeChangeHours",
        when(col("timeChange") > 0, col("timeChange") / 3600)
          otherwise null)

      //calculate distance travelled over the time window
      .withColumn("startDistance", min("cumulativeDistanceKm") over rollingTimeWindow)
      .withColumn("endDistance", max("cumulativeDistanceKm") over rollingTimeWindow)
      .withColumn("distanceChange",
        when (col("endDistance") - col("startDistance") > 0, col("endDistance") - col("startDistance"))
        otherwise null)
      //calculate speed based on time and distance
      .withColumn("smoothSpeedKmH", (col("distanceChange") / col("timeChangeHours")).cast("double"))

      //calculate elevation change over the time window
      .withColumn("startElevation", min("elevation") over rollingTimeWindow)
      .withColumn("endElevation", max("elevation") over rollingTimeWindow)
      .withColumn("elevationChange", col("endElevation") - col("startElevation"))
      //calculate gradient based on distance and elevation change. Distance is * 1000 as it needs to be converted to m rather than km
      .withColumn("gradient", col("elevationChange") / (col("distanceChange") * 1000))

      //drop intermediary columns
      .drop("startDistance", "endDistance", "distanceChange", "startTime", "endTime", "timeChange", "timeChangeHours", "startElevation", "endElevation", "elevationChange")
  }


}
