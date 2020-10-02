import scala.language.implicitConversions
import org.apache.spark.sql.{Dataset, Row, SparkSession, functions, expressions}


object Analyser {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("GPXAnalyser")
      .master("local[3]")
      .getOrCreate()

    val testActivityDataset : Dataset[ActivityFileParser.ActivityRecord] = ActivityFileParser.readGPXToDataFrame("Data/activity_4900763877.gpx", spark);

    testActivityDataset.show


  }
}
