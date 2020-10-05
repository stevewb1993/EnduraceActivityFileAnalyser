package com.stevebowser.enduranceactivityfileanalyser.fileparser

import CommonTermsStandardiser.matchActivityType
import org.apache.spark.sql.execution.streaming.FileStreamSource.Timestamp
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, input_file_name, posexplode, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import DistanceCalculator.addCumulativeStatistics
import SpeedCalculator.addSmoothedSpeed

object FileParser {

  private val matchActivityTypeUDF: UserDefinedFunction = udf(matchActivityType(_:String):String)

  //thiis is the standardised output of the dataset from calls to parse activity files
  case class ActivityRecord(
                              activityName: String,
                              originalActivityType: String,
                              activityTrackPoint: Integer,
                              activityID: String,
                              parsedActivityType: String,
                              time: Timestamp,
                              latitude: Double,
                              longitude: Double,
                              heartRate: Integer,
                              cadence: Integer,
                              power: Integer,
                              cumulativeDistanceKm: Double,
                              cumulativeTime: Long,
                              smoothSpeedKmH: Double
                            )

  def readGPXToDataFrame (path: String, spark: SparkSession) : Dataset[ActivityRecord] = {
    //define schema of GPX file so data types are read correctly
    val rawGPXSchema =
      StructType(Seq(
        StructField("_creator",StringType,nullable = true)
        , StructField("_ns2",StringType,nullable = true)
        , StructField("_ns3",StringType,nullable = true)
        , StructField("_schemaLocation",StringType,nullable = true)
        , StructField("_version",DoubleType,nullable = true)
        , StructField("_xmlns",StringType,nullable = true)
        , StructField("_xsi",StringType,nullable = true)
        , StructField("metadata",
          StructType(Seq(
            StructField("link",
              StructType(Seq(
                StructField("_href",StringType,nullable = true)
                , StructField("text",StringType,nullable = true))), nullable = true)
            , StructField("time",TimestampType,nullable = true))), nullable = true)
        ,  StructField("trk",
          StructType(Seq(
            StructField("name",StringType,nullable = true)
            , StructField("type",StringType,nullable = true)
            , StructField("trkseg",
              StructType(Seq(
                StructField("trkpt",ArrayType(
                  StructType(Seq(
                    StructField("_lat",DoubleType,nullable = true)
                    , StructField("_lon",DoubleType,nullable = true)
                    , StructField("ele",DoubleType,nullable = true)
                    , StructField("time",TimestampType,nullable = true)
                    , StructField("extensions",
                      StructType(Seq(StructField("ns3:TrackPointExtension",
                        StructType(Seq(StructField("ns3:cad",IntegerType,nullable = true)
                          , StructField("ns3:hr",IntegerType,nullable = true)
                          , StructField("ns3:power",IntegerType,nullable = true))),nullable = true))),nullable = true)))),nullable = true))),nullable = true))),nullable = true)))


    //import spark implicits for cast to dataset
    import spark.implicits._

    //this function parses the GPX data into standardised format
    def unNestGPXDf (df: DataFrame) : DataFrame = df
      //Data is read in at the top level of the json. All of the data of interest is deeply nested so needs to be exploded to facilitate aggregations
      .select(col("trk.name"),col("trk.type"),posexplode(col("trk.trkseg.trkpt")))
      //rename the output of the exploded column so it is more descriptive
      .withColumnRenamed("col", "trackPointDetails")
      //use input file name as unique id for the activity since there is not one in the file
      .withColumn("activityId", input_file_name())
      .withColumnRenamed("trk.name", "activityName")
      .withColumnRenamed("trk.type", "originalActivityType")
      //attempt to parse the activity type to standard naming convention. Returns unknown if not available
      .withColumn("parsedActivityType", matchActivityTypeUDF(col("originalActivityType")))
      //this is the relative position of each sensor output in the activity
      .withColumnRenamed("pos", "activityTrackPoint")
      .withColumn("time", col("trackPointDetails.time"))
      .withColumn("latitude", col("trackPointDetails._lat"))
      .withColumn("longitude", col("trackPointDetails._lon"))
      .withColumn("heartRate", col("trackPointDetails.extensions.ns3:TrackPointExtension.ns3:hr"))
      .withColumn("cadence", col("trackPointDetails.extensions.ns3:TrackPointExtension.ns3:cad"))
      .withColumn("power", col("trackPointDetails.extensions.ns3:TrackPointExtension.ns3:power"))
      //all columns of interest have been extracted, so this struct column can now be dropped
      .drop("trackPointDetails")

    //read the dataframe from the requested path
    val rawGPXDf =
      spark.read
        .format("com.databricks.spark.xml")
        .option("rowTag", "gpx")
        .schema(rawGPXSchema)
        .load(path)

    //unnest the dataframe
    val unNestedGPXDf = unNestGPXDf(rawGPXDf)

    //add cumulative statistics and return
    val withDistanceDf = addCumulativeStatistics(unNestedGPXDf)

    //add smoothed speed and return
    addSmoothedSpeed(withDistanceDf).as[ActivityRecord]

  }


}
