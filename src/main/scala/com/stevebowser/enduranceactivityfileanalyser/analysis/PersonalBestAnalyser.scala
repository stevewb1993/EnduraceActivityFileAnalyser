package com.stevebowser.enduranceactivityfileanalyser.analysis

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.{max, _}

object PersonalBestAnalyser {

  def calculateDistancePersonalBests(ds: Dataset[ActivityRecord], distance : Int) : DataFrame = {

    //in order to calculate the personal bests in each activity, we must join the activity table onto itself to find all rows in the activity which have past the required distance
    val allPointsGreaterThanRequiredDistance = genericActivityJoin(ds, distance, "cumulativeDistanceKm","outside")

    //within each point, we can now find the latest historic point for each point in the activity that was the mimimum distance away
    val latestPointGreaterThanRequiredDistance = allPointsGreaterThanRequiredDistance
      .groupBy("activityId", "activityTrackPoint", "cumulativeTime", "parsedActivityType")
      .max("cumulativeTime2").alias("cumulativeTime2")
      .withColumnRenamed("max(cumulativeTime2)", "cumulativeTime2")
      .withColumn("timeDifference", col("cumulativeTime") - col("cumulativeTime2"))

    //now find the minimum time difference for the distance for each activity type
    val fastestTimes = latestPointGreaterThanRequiredDistance
      .groupBy("parsedActivityType")
      .min("timeDifference")

    fastestTimes
  }

  def calculateSensorPersonalBests (ds: Dataset[ActivityRecord], timeInterval : Long) : DataFrame = {

    //in order to calculate the personal bests in each activity, we must join the activity table onto itself to find all rows in the activity which have past the required time interval
    val allPointsGreaterThanRequiredTime = genericActivityJoin(ds, timeInterval, "cumulativeTime", "within")

    //within each point we can now calculate the average values for the sensor data over the requested time window
    val latestPointGreaterThanRequiredTime = allPointsGreaterThanRequiredTime
      //remove cases where the window being averaged is not greater than the requested time interval
      .filter(col("cumulativeTime") > timeInterval)
      //calculate rolling overages
      .groupBy("activityId", "activityTrackPoint", "parsedActivityType")
      .agg(mean("heartRate2").alias("rollingHeartRate"),
        mean("power2").alias("rollingPower"),
        mean("cadence2").alias("rollingCadence")
      )
    //find the highest and lowest average over the time window
    latestPointGreaterThanRequiredTime
      .groupBy("parsedActivityType")
      .agg(
        min("rollingCadence").alias("minCadence"),
        max("rollingCadence").alias("maxCadence"),
        min("rollingHeartRate").alias("minHeartRate"),
        max("rollingHeartRate").alias("maxHeartRate"),
        min("rollingPower").alias("minPower") ,
        max("rollingPower").alias("maxPower"))

  }


  private def genericActivityJoin (ds : Dataset[ActivityRecord], intervalLength : Long, intervalType : String, withinOrOutSideInterval : String) = {

    //intervalType must be set to either cumulativeTime or CumulativeDistance.
    //interval length must be > 0
    //Figure out how to do error handling for this

    //in order to calculate the personal bests in each activity, we must join the activity table onto itself to find all rows in the activity which have past the required distance
    val ds2 = ds
      //rename required columns to prevent clashes
      .withColumnRenamed("activityId", "activityId2")
      .withColumnRenamed("activityTrackPoint", "activityTrackPoint2")
      .withColumnRenamed("cumulativeDistanceKm", "cumulativeDistanceKm2")
      .withColumnRenamed("parsedActivityType", "parsedActivityType2")
      .withColumnRenamed("cumulativeTime", "cumulativeTime2")
      .withColumnRenamed("heartRate", "heartRate2")
      .withColumnRenamed("cadence", "cadence2")
      .withColumnRenamed("power", "power2")


    val selfJoinExpression = if (withinOrOutSideInterval == "within") {
      ds.col("activityId") === ds2.col("activityId2") and
      ds.col("activityTrackPoint") > ds2.col("activityTrackPoint2") and
      ds.col(intervalType) < ds2.col(intervalType + 2) + intervalLength
    }
    else {
      ds.col("activityId") === ds2.col("activityId2") and
      ds.col("activityTrackPoint") > ds2.col("activityTrackPoint2") and
      ds.col(intervalType) > ds2.col(intervalType + 2) + intervalLength
    }

    ds.join(ds2,selfJoinExpression,"inner");

  }

}
