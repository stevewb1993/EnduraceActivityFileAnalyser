package com.stevebowser.enduranceactivityfileanalyser.fileparser

import java.sql.Timestamp

import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import DistanceCalculator.addCumulativeStatistics
import org.apache.spark.sql.sources

class DistanceCalculatorTest extends AnyFlatSpec with BeforeAndAfterAll {

  var spark: SparkSession = _
  var cumulativeStatsDf: DataFrame = _

  override def beforeAll(): Unit = {

    //create spark session
    val spark = SparkSession.builder()
      .appName("SparkTest")
      .master("local[3]")
      .getOrCreate()

    //insert data into test dataframe
    val testDataSchema : StructType =
      StructType(Seq(
        StructField("time", TimestampType)
        , StructField("latitude",DoubleType)
        , StructField("longitude",DoubleType)
        , StructField("activityTrackPoint", IntegerType)
        , StructField("activityId",StringType)))


    val time1 : Timestamp = new Timestamp(1601818638L ) //"2020-05-08T09:19:40.000Z")
    val time2 : Timestamp = new Timestamp(1601878638L ) // Timestamp = new Timestamp(System.currentTimeMillis()). //= "2020-05-08T09:20:40.000Z"
    val latitude1 = 40.7491
    val latitude2 = 40.7401
    val longitude1 = -73.9864
    val longitude2 = -73.9864
    val activityTrackPoint1 = 1
    val activityTrackPoint2 = 2
    val activityId = "genericActivity"

    val testData = Seq(
      Row(time1, latitude1, longitude1, activityTrackPoint1, activityId),
      Row(time2, latitude2, longitude2, activityTrackPoint2, activityId)
    )

    val testInputDataFrame = spark.createDataFrame(
      spark.sparkContext.parallelize(testData),
      testDataSchema
    )

    cumulativeStatsDf  = addCumulativeStatistics(testInputDataFrame)

  }

  override def afterAll(): Unit = {
    spark.stop()
  }

  "addCumulativeStatistics" should "show a cumulative time of 60 (seconds) when difference in time is 1 minute" in {

    //act
    val expectedValue = 60L;
    val actualValue : Long = cumulativeStatsDf.filter("activityTrackPoint = 2").select("cumulativeTime").first().get(0).toString.toLong
    //assert
    assert(expectedValue == actualValue)

  }

  it should "show a cumulative distance of 1km for the appropriate latitude/longitude" in {
    //act
    val expectedValue = 1D
    //actuall value is rounded since it's tricky to get lat/longitude combos that are exactly 1km different
    val actualValue : Double = Math.round(cumulativeStatsDf.filter("activityTrackPoint = 2").select("cumulativeDistanceKm").first().get(0).toString.toDouble)
    //assert
    assert(expectedValue == actualValue)
  }

}
