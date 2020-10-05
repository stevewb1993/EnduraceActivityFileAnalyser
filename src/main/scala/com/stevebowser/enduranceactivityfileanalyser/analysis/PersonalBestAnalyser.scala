package com.stevebowser.enduranceactivityfileanalyser.analysis

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{max, _}

object PersonalBestAnalyser {

  def calculateDistancePersonalBests(ds: Dataset[ActivityRecord], distance : Int) : DataFrame = {

    //create window for each activity that represents the distance window specified
    val rollingActivityTDistanceWindow = Window.partitionBy("activityId").orderBy("cumulativeDistanceKm").rangeBetween(-distance,0)

    val rollingIntervalTime = ds
      //the min cumulative time in the window represents the start time of the athlete completing the requisite distance relative to their current positition
      .withColumn("startTime", min("cumulativeTime") over(rollingActivityTDistanceWindow))
      //time to complete distance is their current cumulative time minus the start time
      .withColumn("intervalTime", col("cumulativeTime") - col("startTime"))
      //remove cases where the required distance had not been completed
      .filter(col("cumulativeDistanceKm") > distance)

    //now find the minimum time difference for the distance for each activity type
    val fastestTimes = rollingIntervalTime
      .groupBy("parsedActivityType")
      .min("intervalTime")

    fastestTimes
  }

  def calculateSensorPersonalBests (ds: Dataset[ActivityRecord], timeInterval : Long) : DataFrame = {

    //create window for each activity that represents the time window specifed
    val rollingActivityTimeWindow = Window.partitionBy("activityId").orderBy("cumulativeTime").rangeBetween(-timeInterval,0) //the previous two hours

    val rollingSensorData = ds
      .withColumn("rollingPower", avg("power") over(rollingActivityTimeWindow))
      .withColumn("rollingHeartRate", avg("heartRate") over(rollingActivityTimeWindow))
      .withColumn("rollingCadence", avg("cadence") over(rollingActivityTimeWindow))

    //find the highest and lowest average over the time window
    rollingSensorData
      //filter for points where the time in the activity is long enough
      .filter(col("cumulativeTime") > timeInterval)
      .groupBy("parsedActivityType")
      .agg(
        min("rollingCadence").alias("minCadence"),
        max("rollingCadence").alias("maxCadence"),
        min("rollingHeartRate").alias("minHeartRate"),
        max("rollingHeartRate").alias("maxHeartRate"),
        min("rollingPower").alias("minPower") ,
        max("rollingPower").alias("maxPower"))

  }

}
