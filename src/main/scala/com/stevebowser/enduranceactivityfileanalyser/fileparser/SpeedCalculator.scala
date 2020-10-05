package com.stevebowser.enduranceactivityfileanalyser.fileparser

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object SpeedCalculator {

  def addSmoothedSpeed (df : DataFrame) = {

    //window for calculating speed. Look +- 10 seconds
    val rollingTimeWindow = Window.partitionBy("activityId").orderBy("cumulativeTime").rangeBetween(-10, 10)
    df
      //calculate distance travelled over the time window
      .withColumn("startDistance", min("cumulativeDistanceKm") over rollingTimeWindow)
      .withColumn("endDistance", max("cumulativeDistanceKm") over rollingTimeWindow)
      .withColumn("distanceChange", col("endDistance") - col("startDistance"))
      //calculate time change over time window (cannot assume 20 seconds due to edge cases). change into hours so output is km/hour
      .withColumn("startTime", min("cumulativeTime") over rollingTimeWindow)
      .withColumn("endTime", max("cumulativeTime") over rollingTimeWindow)
      .withColumn("timeChange", (col("endTime") - col("startTime")))
      //change timeChange to hours to ensure correct units
      .withColumn("timeChangeHours",
        when(col("timeChange") > 0, col("timeChange") / 3600)
        otherwise null)
      //calculate speed
      .withColumn("smoothSpeedKmH", (col("distanceChange") / col("timeChangeHours")).cast("double"))
      //drop intermediary columns
      .drop("startDistance", "endDistance", "distanceChange", "startTime", "endTime", "timeChange", "timeChangeHours")

  }

}
