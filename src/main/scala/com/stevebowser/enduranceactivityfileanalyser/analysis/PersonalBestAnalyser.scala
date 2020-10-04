package com.stevebowser.enduranceactivityfileanalyser.analysis

import com.stevebowser.enduranceactivityfileanalyser.fileparser.FileParser.ActivityRecord
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object PersonalBestAnalyser {

  def calculatePersonalBests (ds: Dataset[ActivityRecord], distance : Int) : DataFrame = {

    //in order to calculate the personal bests in each activity, we must join the activity table onto itself to find all rows in the activity which have past the required distance
    val ds2 = ds
      //rename required columns to prevent clashes
      .withColumnRenamed("activityId", "activityId2")
      .withColumnRenamed("activityTrackPoint", "activityTrackPoint2")
      .withColumnRenamed("cumulativeDistanceKm", "cumulativeDistanceKm2")
      .withColumnRenamed("parsedActivityType", "parsedActivityType2")
      .withColumnRenamed("cumulativeTime", "cumulativeTime2")


    val selfJoinExpression = ds.col("activityId") === ds2.col("activityId2") and
      ds.col("activityTrackPoint") > (ds2.col("activityTrackPoint2")) and
      ds.col("cumulativeDistanceKm") > ds2.col("cumulativeDistanceKm2") + 5

    val allPointsGreaterThanRequiredDistance = ds.join(ds2,selfJoinExpression,"inner");

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

}
